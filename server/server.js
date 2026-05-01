const http = require('http');
const express = require('express');
const cors = require('cors');
const { Server } = require('socket.io');
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');

const PORT = process.env.PORT || 3000;
const CLIENT_ORIGIN = process.env.CLIENT_ORIGIN || '*';
const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  console.error('DATABASE_URL is required');
  process.exit(1);
}

const app = express();
app.use(express.json());
app.use(cors({ origin: CLIENT_ORIGIN === '*' ? true : CLIENT_ORIGIN, credentials: true }));

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS players (
      id TEXT PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      rating INTEGER NOT NULL DEFAULT 1200,
      peak_rating INTEGER NOT NULL DEFAULT 1200,
      games_played INTEGER NOT NULL DEFAULT 0,
      wins INTEGER NOT NULL DEFAULT 0,
      losses INTEGER NOT NULL DEFAULT 0,
      draws INTEGER NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS rated_games (
      id TEXT PRIMARY KEY,
      room_id TEXT NOT NULL,
      white_player_id TEXT NOT NULL,
      black_player_id TEXT NOT NULL,
      result TEXT NOT NULL CHECK (result IN ('white','black','draw')),
      white_rating_before INTEGER NOT NULL,
      white_rating_after INTEGER NOT NULL,
      black_rating_before INTEGER NOT NULL,
      black_rating_after INTEGER NOT NULL,
      played_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS player_vs_daily (
      day_key TEXT NOT NULL,
      player_a TEXT NOT NULL,
      player_b TEXT NOT NULL,
      games_count INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (day_key, player_a, player_b)
    );
  `);

  console.log('DB ready');
}

function expectedScore(rA, rB) {
  return 1 / (1 + Math.pow(10, (rB - rA) / 400));
}

function kFactor(rating, gamesPlayed) {
  if (gamesPlayed < 30) return 40;
  if (rating >= 2000) return 10;
  return 20;
}

function rankTitle(rating) {
  if (rating >= 2400) return 'Бездна';
  if (rating >= 2200) return 'Властелин';
  if (rating >= 2000) return 'Архитектор';
  if (rating >= 1800) return 'Хранитель';
  if (rating >= 1600) return 'Мастер';
  if (rating >= 1400) return 'Странник';
  if (rating >= 1200) return 'Свет';
  if (rating >= 1000) return 'Полумрак';
  if (rating >= 800) return 'Сумерки';
  return 'Тьма';
}

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: CLIENT_ORIGIN === '*' ? true : CLIENT_ORIGIN, methods: ['GET', 'POST'] }
});

const queue = [];
const activeGames = new Map(); // roomId -> state
const socketToPlayer = new Map(); // socket.id -> playerId
const playerToSocket = new Map(); // playerId -> socket.id
const usernameToPlayer = new Map(); // usernameLower -> playerId
const pendingInvites = new Map(); // inviteId -> {fromPlayerId,toPlayerId,createdAt}

function clearQueueForPlayer(playerId) {
  const idx = queue.findIndex((x) => x.playerId === playerId);
  if (idx >= 0) queue.splice(idx, 1);
}

function findMatchmakingPair() {
  if (queue.length < 2) return null;
  const a = queue.shift();
  const b = queue.shift();
  return [a, b];
}

function todayKey() {
  return new Date().toISOString().slice(0, 10);
}

function pairKey(p1, p2) {
  return p1 < p2 ? [p1, p2] : [p2, p1];
}

async function canPlayRated(p1, p2) {
  const [a, b] = pairKey(p1, p2);
  const day = todayKey();
  const res = await pool.query(
    `SELECT games_count FROM player_vs_daily WHERE day_key = $1 AND player_a = $2 AND player_b = $3`,
    [day, a, b]
  );
  return (res.rows[0]?.games_count || 0) < 3;
}

async function increasePairDailyCount(p1, p2) {
  const [a, b] = pairKey(p1, p2);
  const day = todayKey();
  await pool.query(
    `
      INSERT INTO player_vs_daily(day_key, player_a, player_b, games_count)
      VALUES ($1, $2, $3, 1)
      ON CONFLICT(day_key, player_a, player_b)
      DO UPDATE SET games_count = player_vs_daily.games_count + 1
    `,
    [day, a, b]
  );
}

function createRoomAndNotifyPlayers({
  whitePlayerId,
  blackPlayerId,
  whiteSocketId,
  blackSocketId,
  mode = 'ranked'
}) {
  const roomId = `room_${uuidv4()}`;
  const game = {
    roomId,
    mode,
    whitePlayerId,
    blackPlayerId,
    whiteSocketId,
    blackSocketId,
    turn: 'white',
    movesCount: 0,
    finished: false,
    createdAt: Date.now()
  };
  activeGames.set(roomId, game);
  io.sockets.sockets.get(whiteSocketId)?.join(roomId);
  io.sockets.sockets.get(blackSocketId)?.join(roomId);

  io.to(roomId).emit('match_found', {
    roomId,
    mode,
    white: { playerId: whitePlayerId },
    black: { playerId: blackPlayerId }
  });
}

app.get('/health', async (_req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ ok: true, service: 'bezdna-api' });
  } catch (e) {
    res.status(500).json({ ok: false, error: 'db_unavailable' });
  }
});

io.on('connection', (socket) => {
  socket.on('register', async ({ playerId, username }) => {
    try {
      if (!playerId || !username) {
        socket.emit('error_message', { message: 'playerId и username обязательны' });
        return;
      }

      const normalized = String(username).trim().slice(0, 30);
      if (!normalized) {
        socket.emit('error_message', { message: 'Никнейм пустой' });
        return;
      }

      await pool.query(
        `
          INSERT INTO players(id, username)
          VALUES ($1, $2)
          ON CONFLICT(id) DO UPDATE
          SET username = EXCLUDED.username, updated_at = NOW()
        `,
        [playerId, normalized]
      );

      socketToPlayer.set(socket.id, playerId);
      playerToSocket.set(playerId, socket.id);
      usernameToPlayer.set(normalized.toLowerCase(), playerId);

      const p = await pool.query(`SELECT * FROM players WHERE id = $1`, [playerId]);
      const row = p.rows[0];
      socket.emit('registered', {
        playerId: row.id,
        username: row.username,
        rating: row.rating,
        title: rankTitle(row.rating)
      });
    } catch (e) {
      socket.emit('error_message', { message: 'Ошибка регистрации' });
    }
  });

  socket.on('queue_join', async ({ mode = 'ranked' } = {}) => {
    try {
      const playerId = socketToPlayer.get(socket.id);
      if (!playerId) return socket.emit('error_message', { message: 'Сначала register' });

      clearQueueForPlayer(playerId);
      const pRes = await pool.query(`SELECT id, rating FROM players WHERE id = $1`, [playerId]);
      if (!pRes.rows.length) return socket.emit('error_message', { message: 'Игрок не найден' });

      queue.push({
        socketId: socket.id,
        playerId,
        mode,
        joinedAt: Date.now(),
        rating: pRes.rows[0].rating
      });
      socket.emit('queue_status', { inQueue: true, size: queue.length });

      const pair = findMatchmakingPair();
      if (!pair) return;

      const [a, b] = pair;
      if (a.mode === 'ranked' && b.mode === 'ranked') {
        const allowed = await canPlayRated(a.playerId, b.playerId);
        if (!allowed) {
          io.to(a.socketId).emit('error_message', { message: 'Лимит рейтинговых игр с этим соперником на сегодня достигнут' });
          io.to(b.socketId).emit('error_message', { message: 'Лимит рейтинговых игр с этим соперником на сегодня достигнут' });
          return;
        }
      }

      const white = Math.random() < 0.5 ? a : b;
      const black = white === a ? b : a;
      createRoomAndNotifyPlayers({
        whitePlayerId: white.playerId,
        blackPlayerId: black.playerId,
        whiteSocketId: white.socketId,
        blackSocketId: black.socketId,
        mode: a.mode === 'ranked' && b.mode === 'ranked' ? 'ranked' : 'casual'
      });
    } catch (e) {
      socket.emit('error_message', { message: 'Ошибка queue_join' });
    }
  });

  socket.on('queue_leave', () => {
    const playerId = socketToPlayer.get(socket.id);
    if (!playerId) return;
    clearQueueForPlayer(playerId);
    socket.emit('queue_status', { inQueue: false, size: queue.length });
  });

  socket.on('invite_by_nick', async ({ targetNickname }) => {
    try {
      const fromPlayerId = socketToPlayer.get(socket.id);
      if (!fromPlayerId) return socket.emit('invite_result', { status: 'error', message: 'Сначала register' });

      const target = String(targetNickname || '').trim().toLowerCase();
      if (!target) return socket.emit('invite_result', { status: 'error', message: 'Укажите ник' });

      const toPlayerId = usernameToPlayer.get(target);
      if (!toPlayerId) return socket.emit('invite_result', { status: 'not_found' });
      if (toPlayerId === fromPlayerId) return socket.emit('invite_result', { status: 'error', message: 'Нельзя пригласить себя' });

      const toSocketId = playerToSocket.get(toPlayerId);
      if (!toSocketId) return socket.emit('invite_result', { status: 'not_found' });

      clearQueueForPlayer(fromPlayerId);
      clearQueueForPlayer(toPlayerId);

      const inviteId = uuidv4();
      pendingInvites.set(inviteId, {
        fromPlayerId,
        toPlayerId,
        createdAt: Date.now()
      });

      const fromPlayer = await pool.query(`SELECT username FROM players WHERE id = $1`, [fromPlayerId]);
      const fromNickname = fromPlayer.rows[0]?.username || 'Игрок';

      io.to(toSocketId).emit('invite_received', { inviteId, fromPlayerId, fromNickname });
      socket.emit('invite_result', { status: 'sent', targetNickname });

      setTimeout(() => {
        const inv = pendingInvites.get(inviteId);
        if (!inv) return;
        pendingInvites.delete(inviteId);
        const fromSock = playerToSocket.get(inv.fromPlayerId);
        if (fromSock) io.to(fromSock).emit('invite_result', { status: 'expired' });
      }, 60000);
    } catch (e) {
      socket.emit('invite_result', { status: 'error', message: 'Ошибка приглашения' });
    }
  });

  socket.on('invite_response', ({ inviteId, accept }) => {
    const invite = pendingInvites.get(inviteId);
    if (!invite) return;

    const responderId = socketToPlayer.get(socket.id);
    if (!responderId || responderId !== invite.toPlayerId) return;

    pendingInvites.delete(inviteId);
    const inviterSocketId = playerToSocket.get(invite.fromPlayerId);
    if (!inviterSocketId) return;

    if (!accept) {
      io.to(inviterSocketId).emit('invite_result', { status: 'declined' });
      return;
    }

    const inviterIsWhite = Math.random() < 0.5;
    createRoomAndNotifyPlayers({
      whitePlayerId: inviterIsWhite ? invite.fromPlayerId : invite.toPlayerId,
      blackPlayerId: inviterIsWhite ? invite.toPlayerId : invite.fromPlayerId,
      whiteSocketId: inviterIsWhite ? inviterSocketId : socket.id,
      blackSocketId: inviterIsWhite ? socket.id : inviterSocketId,
      mode: 'private'
    });
    io.to(inviterSocketId).emit('invite_result', { status: 'accepted' });
  });

  socket.on('make_move', ({ roomId, move }) => {
    const game = activeGames.get(roomId);
    if (!game || game.finished) return;

    const playerId = socketToPlayer.get(socket.id);
    if (!playerId) return;
    const side = playerId === game.whitePlayerId ? 'white' : playerId === game.blackPlayerId ? 'black' : null;
    if (!side) return;
    if (game.turn !== side) return socket.emit('error_message', { message: 'Не ваш ход' });

    game.movesCount += 1;
    game.turn = game.turn === 'white' ? 'black' : 'white';
    socket.to(roomId).emit('opponent_move', { roomId, move, by: playerId });
    io.to(roomId).emit('turn_changed', { turn: game.turn, movesCount: game.movesCount });
  });

  socket.on('game_end', async ({ roomId, result }) => {
    const game = activeGames.get(roomId);
    if (!game || game.finished) return;
    game.finished = true;
    if (!['white', 'black', 'draw'].includes(result)) return;

    if (game.mode === 'ranked') {
      try {
        const whiteRes = await pool.query(`SELECT * FROM players WHERE id = $1`, [game.whitePlayerId]);
        const blackRes = await pool.query(`SELECT * FROM players WHERE id = $1`, [game.blackPlayerId]);
        if (!whiteRes.rows.length || !blackRes.rows.length) return;

        const w = whiteRes.rows[0];
        const b = blackRes.rows[0];
        const Ew = expectedScore(w.rating, b.rating);
        const Eb = expectedScore(b.rating, w.rating);
        const Sw = result === 'white' ? 1 : result === 'draw' ? 0.5 : 0;
        const Sb = result === 'black' ? 1 : result === 'draw' ? 0.5 : 0;
        const Kw = kFactor(w.rating, w.games_played);
        const Kb = kFactor(b.rating, b.games_played);
        const wNew = Math.max(0, Math.round(w.rating + Kw * (Sw - Ew)));
        const bNew = Math.max(0, Math.round(b.rating + Kb * (Sb - Eb)));

        await pool.query('BEGIN');
        await pool.query(
          `UPDATE players
           SET rating=$2, peak_rating=GREATEST(peak_rating,$2), games_played=games_played+1,
               wins=wins+$3, losses=losses+$4, draws=draws+$5, updated_at=NOW()
           WHERE id=$1`,
          [w.id, wNew, result === 'white' ? 1 : 0, result === 'black' ? 1 : 0, result === 'draw' ? 1 : 0]
        );
        await pool.query(
          `UPDATE players
           SET rating=$2, peak_rating=GREATEST(peak_rating,$2), games_played=games_played+1,
               wins=wins+$3, losses=losses+$4, draws=draws+$5, updated_at=NOW()
           WHERE id=$1`,
          [b.id, bNew, result === 'black' ? 1 : 0, result === 'white' ? 1 : 0, result === 'draw' ? 1 : 0]
        );
        await pool.query(
          `INSERT INTO rated_games(
            id, room_id, white_player_id, black_player_id, result,
            white_rating_before, white_rating_after, black_rating_before, black_rating_after
          ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
          [uuidv4(), roomId, w.id, b.id, result, w.rating, wNew, b.rating, bNew]
        );
        await increasePairDailyCount(w.id, b.id);
        await pool.query('COMMIT');

        io.to(roomId).emit('rating_updated', {
          white: { playerId: w.id, oldRating: w.rating, newRating: wNew, delta: wNew - w.rating, title: rankTitle(wNew) },
          black: { playerId: b.id, oldRating: b.rating, newRating: bNew, delta: bNew - b.rating, title: rankTitle(bNew) }
        });
      } catch (e) {
        try { await pool.query('ROLLBACK'); } catch (_) {}
      }
    }

    io.to(roomId).emit('game_finished', { roomId, result, rated: game.mode === 'ranked' });
    activeGames.delete(roomId);
  });

  socket.on('disconnect', () => {
    const playerId = socketToPlayer.get(socket.id);
    socketToPlayer.delete(socket.id);
    if (!playerId) return;

    clearQueueForPlayer(playerId);
    playerToSocket.delete(playerId);

    for (const [nickLower, pId] of usernameToPlayer.entries()) {
      if (pId === playerId) usernameToPlayer.delete(nickLower);
    }
  });
});

initDb()
  .then(() => server.listen(PORT, () => console.log(`bezdna-api started on :${PORT}`)))
  .catch((e) => {
    console.error('DB init failed:', e);
    process.exit(1);
  });
