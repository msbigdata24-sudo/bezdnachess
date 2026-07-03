'use strict';

const FLEETS = {
  10: [4, 3, 3, 2, 2, 2, 1, 1, 1, 1],
  12: [5, 4, 4, 3, 3, 3, 2, 2, 2, 2, 1, 1, 1, 1],
  14: [6, 5, 5, 4, 4, 3, 3, 3, 3, 2, 2, 2, 2, 1, 1, 1, 1, 1],
  16: [6, 5, 5, 4, 4, 4, 3, 3, 3, 3, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1]
};

const EMPTY = 0;
const SHIP = 1;
const MISS = 2;
const HIT = 3;
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

function createBoard(size) {
  return Array.from({ length: size }, () => Array(size).fill(EMPTY));
}

function randomCode(existing) {
  for (let attempt = 0; attempt < 80; attempt++) {
    let code = '';
    for (let i = 0; i < 6; i++) {
      code += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
    }
    if (!existing.has(code)) return code;
  }
  return `BB${Date.now().toString(36).slice(-4).toUpperCase()}`;
}

function isValidPlacement(board, cells, size) {
  const own = new Set(cells.map(([r, c]) => `${r},${c}`));
  for (const [r, c] of cells) {
    if (r < 0 || r >= size || c < 0 || c >= size) return false;
    if (board[r][c] === SHIP) return false;
  }
  for (const [r, c] of cells) {
    for (let dr = -1; dr <= 1; dr++) {
      for (let dc = -1; dc <= 1; dc++) {
        if (!dr && !dc) continue;
        const nr = r + dr;
        const nc = c + dc;
        if (nr >= 0 && nr < size && nc >= 0 && nc < size) {
          if (board[nr][nc] === SHIP && !own.has(`${nr},${nc}`)) return false;
        }
      }
    }
  }
  return true;
}

function buildBoard(size, ships) {
  const board = createBoard(size);
  for (const ship of ships) {
    for (const [r, c] of ship.cells) board[r][c] = SHIP;
  }
  return board;
}

function fleetMatches(size, ships) {
  const expected = [...FLEETS[size]].sort((a, b) => b - a);
  const got = ships.map((s) => s.len).sort((a, b) => b - a);
  if (expected.length !== got.length) return false;
  return expected.every((v, i) => v === got[i]);
}

function getShipCellsAt(board, hitR, hitC, size) {
  const cells = [];
  const visited = Array.from({ length: size }, () => Array(size).fill(false));
  const stack = [[hitR, hitC]];
  while (stack.length) {
    const [r, c] = stack.pop();
    if (visited[r][c]) continue;
    if (board[r][c] !== SHIP && board[r][c] !== HIT) continue;
    visited[r][c] = true;
    cells.push([r, c]);
    [[0, 1], [0, -1], [1, 0], [-1, 0]].forEach(([dr, dc]) => {
      const nr = r + dr;
      const nc = c + dc;
      if (nr >= 0 && nr < size && nc >= 0 && nc < size && !visited[nr][nc]) {
        if (board[nr][nc] === SHIP || board[nr][nc] === HIT) stack.push([nr, nc]);
      }
    });
  }
  return cells;
}

function isShipSunk(board, shotsBoard, hitR, hitC, size) {
  const cells = getShipCellsAt(board, hitR, hitC, size);
  return cells.length > 0 && cells.every(([r, c]) => shotsBoard[r][c] === HIT);
}

function markSurroundingMiss(shotsBoard, board, shipCells, size) {
  const added = [];
  for (const [r, c] of shipCells) {
    for (let dr = -1; dr <= 1; dr++) {
      for (let dc = -1; dc <= 1; dc++) {
        const nr = r + dr;
        const nc = c + dc;
        if (nr >= 0 && nr < size && nc >= 0 && nc < size) {
          if (shotsBoard[nr][nc] === EMPTY && board[nr][nc] !== SHIP && board[nr][nc] !== HIT) {
            shotsBoard[nr][nc] = MISS;
            added.push([nr, nc]);
          }
        }
      }
    }
  }
  return added;
}

function playerSlot(room, socketId) {
  if (room.players[1].socketId === socketId) return 1;
  if (room.players[2].socketId === socketId) return 2;
  return null;
}

function emitRoomState(nsp, room) {
  const payload = {
    code: room.code,
    boardSize: room.boardSize,
    phase: room.phase,
    turn: room.turn,
    winner: room.winner,
    p1: {
      name: room.players[1].name,
      ready: room.players[1].ready,
      connected: room.players[1].connected
    },
    p2: {
      name: room.players[2].name,
      ready: room.players[2].ready,
      connected: room.players[2].connected
    }
  };
  nsp.to(room.code).emit('bb_room_state', payload);
}

function attachBattleship(io, options = {}) {
  const nsp = io.of(options.namespace || '/battleship');
  const rooms = new Map();
  const socketMeta = new Map();

  function leaveSocket(socket) {
    const meta = socketMeta.get(socket.id);
    if (!meta) return;
    const room = rooms.get(meta.code);
    socketMeta.delete(socket.id);
    if (!room) return;
    const slot = playerSlot(room, socket.id);
    if (!slot) return;
    room.players[slot].connected = false;
    room.players[slot].socketId = null;
    if (room.phase !== 'ended') {
      socket.to(room.code).emit('bb_opponent_left', { slot });
      if (!room.players[1].connected && !room.players[2].connected) {
        rooms.delete(room.code);
      } else {
        emitRoomState(nsp, room);
      }
    }
    socket.leave(room.code);
  }

  nsp.on('connection', (socket) => {
    socket.on('bb_create_room', ({ name, boardSize }) => {
      const size = Number(boardSize) || 10;
      if (!FLEETS[size]) {
        return socket.emit('bb_error', { message: 'Недопустимый размер поля' });
      }
      leaveSocket(socket);
      const code = randomCode(rooms);
      const room = {
        code,
        boardSize: size,
        phase: 'waiting',
        turn: 1,
        winner: null,
        shotCount: 0,
        players: {
          1: {
            socketId: socket.id,
            name: String(name || 'Игрок 1').slice(0, 24),
            ships: null,
            board: null,
            shots: createBoard(size),
            ready: false,
            connected: true
          },
          2: {
            socketId: null,
            name: '',
            ships: null,
            board: null,
            shots: createBoard(size),
            ready: false,
            connected: false
          }
        }
      };
      rooms.set(code, room);
      socketMeta.set(socket.id, { code, slot: 1 });
      socket.join(code);
      socket.emit('bb_joined', {
        slot: 1,
        code,
        boardSize: size,
        phase: room.phase,
        isHost: true,
        inviteUrl: `${options.frontendBase || 'https://bezdnachess.com'}/morskoy-boy.html?room=${code}`
      });
      emitRoomState(nsp, room);
    });

    socket.on('bb_join_room', ({ name, roomCode }) => {
      const code = String(roomCode || '').trim().toUpperCase();
      const room = rooms.get(code);
      if (!room) return socket.emit('bb_error', { message: 'Комната не найдена. Проверьте код.' });
      if (room.phase === 'ended') return socket.emit('bb_error', { message: 'Эта партия уже завершена.' });

      leaveSocket(socket);

      let slot = playerSlot(room, socket.id);
      if (!slot) {
        if (room.players[2].connected && room.players[2].socketId !== socket.id) {
          return socket.emit('bb_error', { message: 'Комната уже полная.' });
        }
        if (!room.players[2].connected || !room.players[2].socketId) {
          slot = 2;
          room.players[2].socketId = socket.id;
          room.players[2].name = String(name || 'Игрок 2').slice(0, 24);
          room.players[2].connected = true;
          if (room.phase === 'waiting') room.phase = 'setup';
          socket.to(code).emit('bb_opponent_joined', { name: room.players[2].name, slot: 2 });
        } else {
          return socket.emit('bb_error', { message: 'Не удалось войти в комнату.' });
        }
      } else {
        room.players[slot].connected = true;
        room.players[slot].socketId = socket.id;
      }

      socketMeta.set(socket.id, { code, slot });
      socket.join(code);
      socket.emit('bb_joined', {
        slot,
        code,
        boardSize: room.boardSize,
        phase: room.phase,
        isHost: slot === 1,
        opponentName: slot === 1 ? room.players[2].name : room.players[1].name,
        inviteUrl: `${options.frontendBase || 'https://bezdnachess.com'}/morskoy-boy.html?room=${code}`
      });
      emitRoomState(nsp, room);
    });

    socket.on('bb_place_ships', ({ ships }) => {
      const meta = socketMeta.get(socket.id);
      if (!meta) return socket.emit('bb_error', { message: 'Сначала создайте или войдите в комнату.' });
      const room = rooms.get(meta.code);
      if (!room || room.phase === 'ended') return;
      const slot = playerSlot(room, socket.id);
      if (!slot) return;
      if (room.phase !== 'setup' && room.phase !== 'waiting') {
        return socket.emit('bb_error', { message: 'Сейчас нельзя расставлять корабли.' });
      }
      if (!Array.isArray(ships) || !fleetMatches(room.boardSize, ships)) {
        return socket.emit('bb_error', { message: 'Неверный состав флота.' });
      }

      const board = createBoard(room.boardSize);
      for (const ship of ships) {
        if (!Array.isArray(ship.cells) || ship.len !== ship.cells.length) {
          return socket.emit('bb_error', { message: 'Ошибка данных корабля.' });
        }
        if (!isValidPlacement(board, ship.cells, room.boardSize)) {
          return socket.emit('bb_error', { message: 'Корабли соприкасаются или выходят за поле.' });
        }
        for (const [r, c] of ship.cells) board[r][c] = SHIP;
      }

      room.players[slot].ships = ships;
      room.players[slot].board = board;
      room.players[slot].ready = true;
      room.phase = 'setup';

      nsp.to(room.code).emit('bb_setup_update', {
        p1Ready: room.players[1].ready,
        p2Ready: room.players[2].ready
      });

      if (room.players[1].ready && room.players[2].ready) {
        room.phase = 'battle';
        room.turn = 1;
        room.shotCount = 0;
        nsp.to(room.code).emit('bb_battle_start', {
          firstTurn: 1,
          boardSize: room.boardSize
        });
      }
      emitRoomState(nsp, room);
    });

    socket.on('bb_shot', ({ r, c }) => {
      const meta = socketMeta.get(socket.id);
      if (!meta) return;
      const room = rooms.get(meta.code);
      if (!room || room.phase !== 'battle') return;
      const shooter = playerSlot(room, socket.id);
      if (!shooter || room.turn !== shooter) {
        return socket.emit('bb_error', { message: 'Сейчас не ваш ход.' });
      }
      const target = shooter === 1 ? 2 : 1;
      const row = Number(r);
      const col = Number(c);
      const size = room.boardSize;
      if (!Number.isInteger(row) || !Number.isInteger(col) || row < 0 || col < 0 || row >= size || col >= size) {
        return socket.emit('bb_error', { message: 'Некорректный выстрел.' });
      }

      const shotsBoard = room.players[shooter].shots;
      if (shotsBoard[row][col] === HIT || shotsBoard[row][col] === MISS) {
        return socket.emit('bb_error', { message: 'В эту клетку уже стреляли.' });
      }

      const targetBoard = room.players[target].board;
      room.shotCount += 1;
      let result = 'miss';
      let surround = [];

      if (targetBoard[row][col] === SHIP) {
        targetBoard[row][col] = HIT;
        shotsBoard[row][col] = HIT;
        const sunk = isShipSunk(targetBoard, shotsBoard, row, col, size);
        if (sunk) {
          result = 'sunk';
          surround = markSurroundingMiss(shotsBoard, targetBoard, getShipCellsAt(targetBoard, row, col, size), size);
        } else {
          result = 'hit';
        }
      } else {
        shotsBoard[row][col] = MISS;
      }

      let winner = null;
      let allSunk = true;
      for (let i = 0; i < size; i++) {
        for (let j = 0; j < size; j++) {
          if (targetBoard[i][j] === SHIP) {
            allSunk = false;
            break;
          }
        }
        if (!allSunk) break;
      }
      if (allSunk) {
        winner = shooter;
        room.phase = 'ended';
        room.winner = shooter;
      } else if (result === 'miss') {
        room.turn = target;
      }

      nsp.to(room.code).emit('bb_shot_result', {
        shooter,
        r: row,
        c: col,
        result,
        surround,
        turn: room.turn,
        winner,
        shotCount: room.shotCount
      });
      emitRoomState(nsp, room);
    });

    socket.on('disconnect', () => leaveSocket(socket));
  });

  return nsp;
}

module.exports = { attachBattleship, FLEETS };
