import json
import re
import sqlite3
from typing import Any


def get_connection(database_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(database_path)
    conn.row_factory = sqlite3.Row
    return conn


def _format_ongoing_title(
    source_title: str,
    session_number: int,
    save_count: int,
    version_number: int | None = None,
) -> str:
    base = f"{source_title}-{session_number} ({save_count})"
    if version_number is not None:
        return f"{base} (version {version_number})"
    return base


def _extract_version_number(title: str) -> int | None:
    match = re.search(r"\(version\s+(\d+)\)\s*$", title, flags=re.IGNORECASE)
    if not match:
        return None
    return int(match.group(1))


def _normalize_game_title(
    conn: sqlite3.Connection, game: dict[str, Any]
) -> dict[str, Any]:
    if game.get("game_type") == "ongoing":
        title = str(game.get("title") or "")
        game["title"] = re.sub(r"\(Sparad\s+(\d+)\s+ganger\)", r"(\1)", title)
    return game


def _empty_notes() -> list[list[str]]:
    return [["" for _ in range(9)] for _ in range(9)]


def _empty_grid() -> list[list[str]]:
    return [["" for _ in range(9)] for _ in range(9)]


def _normalize_grid(grid: Any) -> list[list[str]]:
    normalized = _empty_grid()
    if not isinstance(grid, list):
        return normalized

    allowed = {"1", "2", "3", "4", "5", "6", "7", "8", "9"}
    for r in range(min(9, len(grid))):
        row = grid[r] if isinstance(grid[r], list) else []
        for c in range(min(9, len(row))):
            value = str(row[c]).strip() if row[c] is not None else ""
            normalized[r][c] = value if value in allowed else ""
    return normalized


def _find_empty_cell(board: list[list[int]]) -> tuple[int, int] | None:
    for r in range(9):
        for c in range(9):
            if board[r][c] == 0:
                return r, c
    return None


def _can_place_digit(board: list[list[int]], row: int, col: int, digit: int) -> bool:
    for idx in range(9):
        if board[row][idx] == digit or board[idx][col] == digit:
            return False

    box_row = (row // 3) * 3
    box_col = (col // 3) * 3
    for r in range(box_row, box_row + 3):
        for c in range(box_col, box_col + 3):
            if board[r][c] == digit:
                return False
    return True


def _solve_board(board: list[list[int]]) -> bool:
    empty_pos = _find_empty_cell(board)
    if empty_pos is None:
        return True

    row, col = empty_pos
    for digit in range(1, 10):
        if not _can_place_digit(board, row, col, digit):
            continue
        board[row][col] = digit
        if _solve_board(board):
            return True
        board[row][col] = 0
    return False


def _build_solution_grid(grid: list[list[str]]) -> list[list[str]] | None:
    board = [[int(cell) if cell else 0 for cell in row] for row in _normalize_grid(grid)]
    if not _solve_board(board):
        return None
    return [[str(board[r][c]) for c in range(9)] for r in range(9)]


def _safe_load_json(value: Any, fallback: Any) -> Any:
    if not isinstance(value, str) or not value.strip():
        return fallback
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError, ValueError):
        return fallback


def init_db(database_path: str) -> None:
    with get_connection(database_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sudoku_games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                difficulty TEXT NOT NULL,
                game_type TEXT NOT NULL DEFAULT 'base',
                grid_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(sudoku_games)").fetchall()
        }
        legacy_columns = {"row_sums_json", "col_sums_json"}

        # Migrate old schema by recreating the table without legacy sum columns.
        if legacy_columns.intersection(columns):
            conn.execute(
                """
                CREATE TABLE sudoku_games_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    difficulty TEXT NOT NULL,
                    grid_json TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                INSERT INTO sudoku_games_new (id, title, difficulty, grid_json, created_at)
                SELECT id, title, difficulty, grid_json, created_at
                FROM sudoku_games
                """
            )
            conn.execute("DROP TABLE sudoku_games")
            conn.execute("ALTER TABLE sudoku_games_new RENAME TO sudoku_games")

        # Add game_type column for separating base games and ongoing games.
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(sudoku_games)").fetchall()
        }
        if "game_type" not in columns:
            conn.execute(
                "ALTER TABLE sudoku_games ADD COLUMN game_type TEXT NOT NULL DEFAULT 'base'"
            )
            conn.execute(
                "UPDATE sudoku_games SET game_type = 'base' WHERE game_type IS NULL OR game_type = ''"
            )

        # Add elapsed_seconds column for storing game timer.
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(sudoku_games)").fetchall()
        }
        if "elapsed_seconds" not in columns:
            conn.execute(
                "ALTER TABLE sudoku_games ADD COLUMN elapsed_seconds INTEGER NOT NULL DEFAULT 0"
            )

        # Add columns for the new ID system.
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(sudoku_games)").fetchall()
        }
        if "source_game_id" not in columns:
            conn.execute("ALTER TABLE sudoku_games ADD COLUMN source_game_id INTEGER")
        if "session_number" not in columns:
            conn.execute("ALTER TABLE sudoku_games ADD COLUMN session_number INTEGER NOT NULL DEFAULT 0")
        if "save_count" not in columns:
            conn.execute("ALTER TABLE sudoku_games ADD COLUMN save_count INTEGER NOT NULL DEFAULT 0")
        if "notes_json" not in columns:
            conn.execute("ALTER TABLE sudoku_games ADD COLUMN notes_json TEXT NOT NULL DEFAULT '[]'")

        # Track when a game was last saved separately from creation time.
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(sudoku_games)").fetchall()
        }
        if "updated_at" not in columns:
            conn.execute("ALTER TABLE sudoku_games ADD COLUMN updated_at TEXT")
            conn.execute(
                "UPDATE sudoku_games SET updated_at = created_at WHERE updated_at IS NULL OR updated_at = ''"
            )

        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(sudoku_games)").fetchall()
        }
        if "completed_at" not in columns:
            conn.execute("ALTER TABLE sudoku_games ADD COLUMN completed_at TEXT")

        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(sudoku_games)").fetchall()
        }
        if "solution_json" not in columns:
            conn.execute("ALTER TABLE sudoku_games ADD COLUMN solution_json TEXT")

        base_rows = conn.execute(
            """
            SELECT id, grid_json
            FROM sudoku_games
            WHERE game_type = 'base' AND (solution_json IS NULL OR solution_json = '')
            """
        ).fetchall()
        for row in base_rows:
            try:
                grid = _normalize_grid(json.loads(row["grid_json"]))
            except json.JSONDecodeError:
                grid = _empty_grid()
            solution = _build_solution_grid(grid)
            if solution is None:
                continue
            conn.execute(
                "UPDATE sudoku_games SET solution_json = ? WHERE id = ?",
                (json.dumps(solution), row["id"]),
            )

        ongoing_rows = conn.execute(
            """
            SELECT id, source_game_id
            FROM sudoku_games
            WHERE game_type = 'ongoing'
              AND source_game_id IS NOT NULL
              AND (solution_json IS NULL OR solution_json = '')
            """
        ).fetchall()
        for row in ongoing_rows:
            source_row = conn.execute(
                "SELECT solution_json FROM sudoku_games WHERE id = ?",
                (row["source_game_id"],),
            ).fetchone()
            source_solution = source_row["solution_json"] if source_row else None
            if not source_solution:
                continue
            conn.execute(
                "UPDATE sudoku_games SET solution_json = ? WHERE id = ?",
                (source_solution, row["id"]),
            )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sudoku_leaderboard (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ongoing_game_id INTEGER NOT NULL UNIQUE,
                source_game_id INTEGER,
                game_title TEXT NOT NULL,
                difficulty TEXT NOT NULL,
                elapsed_seconds INTEGER NOT NULL,
                completed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sudoku_game_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ongoing_game_id INTEGER NOT NULL,
                row_idx INTEGER NOT NULL,
                col_idx INTEGER NOT NULL,
                old_value TEXT NOT NULL,
                new_value TEXT NOT NULL,
                old_notes TEXT NOT NULL,
                new_notes TEXT NOT NULL,
                changed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sudoku_game_inputs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ongoing_game_id INTEGER NOT NULL,
                seq_no INTEGER NOT NULL,
                row_idx INTEGER NOT NULL,
                col_idx INTEGER NOT NULL,
                value TEXT NOT NULL,
                input_kind TEXT NOT NULL DEFAULT 'manual',
                action_name TEXT,
                elapsed_seconds INTEGER NOT NULL DEFAULT 0,
                entered_at TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ongoing_game_id, seq_no)
            )
            """
        )

        input_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(sudoku_game_inputs)").fetchall()
        }
        if "input_kind" not in input_columns:
            conn.execute(
                "ALTER TABLE sudoku_game_inputs ADD COLUMN input_kind TEXT NOT NULL DEFAULT 'manual'"
            )
        if "action_name" not in input_columns:
            conn.execute("ALTER TABLE sudoku_game_inputs ADD COLUMN action_name TEXT")

        conn.commit()


def save_game(
    database_path: str,
    difficulty: str,
    grid: list[list[str]],
    notes: list[list[str]],
    game_type: str,
    elapsed_seconds: int = 0,
    source_game_id: int | None = None,
    include_version_suffix: bool = False,
) -> int:
    with get_connection(database_path) as conn:
        if game_type == "base":
            normalized_grid = _normalize_grid(grid)
            solution_grid = _build_solution_grid(normalized_grid)
            if solution_grid is None:
                raise ValueError("Grundspelet måste ha en giltig lösning.")
            count = conn.execute(
                "SELECT COUNT(*) FROM sudoku_games WHERE game_type = 'base'"
            ).fetchone()[0]
            title = str(count + 1)
            cursor = conn.execute(
                """
                INSERT INTO sudoku_games
                    (title, difficulty, game_type, grid_json, notes_json, elapsed_seconds,
                     source_game_id, session_number, save_count, updated_at, completed_at, solution_json)
                VALUES (?, ?, ?, ?, ?, ?, NULL, 0, 0, CURRENT_TIMESTAMP, NULL, ?)
                """,
                (
                    title,
                    difficulty,
                    game_type,
                    json.dumps(normalized_grid),
                    json.dumps(notes),
                    elapsed_seconds,
                    json.dumps(solution_grid) if solution_grid else None,
                ),
            )
        else:
            source_solution_json = None
            if source_game_id is not None:
                source_solution_row = conn.execute(
                    "SELECT solution_json, grid_json FROM sudoku_games WHERE id = ?",
                    (source_game_id,),
                ).fetchone()
                source_solution_json = (
                    source_solution_row["solution_json"] if source_solution_row else None
                )
                if not source_solution_json and source_solution_row:
                    try:
                        source_grid = _normalize_grid(json.loads(source_solution_row["grid_json"]))
                    except json.JSONDecodeError:
                        source_grid = _empty_grid()
                    source_solution = _build_solution_grid(source_grid)
                    if source_solution is not None:
                        source_solution_json = json.dumps(source_solution)
                        conn.execute(
                            "UPDATE sudoku_games SET solution_json = ? WHERE id = ?",
                            (source_solution_json, source_game_id),
                        )
            source_row = conn.execute(
                "SELECT title FROM sudoku_games WHERE id = ?", (source_game_id,)
            ).fetchone()
            source_title = source_row["title"] if source_row else str(source_game_id)
            session_number = conn.execute(
                "SELECT COUNT(*) FROM sudoku_games WHERE game_type = 'ongoing' AND source_game_id = ?",
                (source_game_id,),
            ).fetchone()[0] + 1
            version_number = (session_number - 1) if include_version_suffix else None
            if version_number is not None and version_number < 1:
                version_number = 1
            title = _format_ongoing_title(source_title, session_number, 0, version_number)
            cursor = conn.execute(
                """
                INSERT INTO sudoku_games
                    (title, difficulty, game_type, grid_json, notes_json, elapsed_seconds,
                     source_game_id, session_number, save_count, updated_at, completed_at, solution_json)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP, NULL, ?)
                """,
                (title, difficulty, game_type, json.dumps(grid), json.dumps(notes), elapsed_seconds,
                 source_game_id, session_number, source_solution_json),
            )
        conn.commit()
        return int(cursor.lastrowid)


def find_base_game_id_by_grid(
    database_path: str,
    grid: list[list[str]],
) -> int | None:
    normalized_grid = _normalize_grid(grid)
    target_grid_json = json.dumps(normalized_grid)

    with get_connection(database_path) as conn:
        row = conn.execute(
            """
            SELECT id
            FROM sudoku_games
            WHERE game_type = 'base' AND grid_json = ?
            ORDER BY id ASC
            LIMIT 1
            """,
            (target_grid_json,),
        ).fetchone()

    return int(row["id"]) if row else None


def update_game(
    database_path: str,
    game_id: int,
    difficulty: str,
    grid: list[list[str]],
    notes: list[list[str]],
    elapsed_seconds: int = 0,
) -> bool:
    with get_connection(database_path) as conn:
        row = conn.execute(
            "SELECT source_game_id, session_number, save_count, title FROM sudoku_games WHERE id = ?",
            (game_id,),
        ).fetchone()
        if row is None:
            return False

        source_game_id = row["source_game_id"]
        session_number = row["session_number"]
        new_save_count = (row["save_count"] or 0) + 1

        source_row = conn.execute(
            "SELECT title FROM sudoku_games WHERE id = ?", (source_game_id,)
        ).fetchone()
        source_title = source_row["title"] if source_row else str(source_game_id)
        existing_title = str(row["title"] or "")
        version_number = _extract_version_number(existing_title)
        new_title = _format_ongoing_title(source_title, session_number, new_save_count, version_number)

        cursor = conn.execute(
            """
            UPDATE sudoku_games
            SET title = ?,
                difficulty = ?,
                grid_json = ?,
                notes_json = ?,
                elapsed_seconds = ?,
                save_count = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (new_title, difficulty, json.dumps(grid), json.dumps(notes), elapsed_seconds, new_save_count, game_id),
        )
        conn.commit()
        return cursor.rowcount > 0


def list_games(database_path: str, game_type: str) -> list[dict[str, Any]]:
    with get_connection(database_path) as conn:
        rows = conn.execute(
            """
                                 SELECT id, title, difficulty, game_type, grid_json, notes_json, elapsed_seconds,
                 source_game_id, session_number, save_count, solution_json,
                  datetime(created_at, 'localtime') AS created_at,
                                    datetime(updated_at, 'localtime') AS updated_at,
                                    datetime(completed_at, 'localtime') AS completed_at
            FROM sudoku_games
                        WHERE game_type = ?
                            AND (? != 'ongoing' OR completed_at IS NULL)
            ORDER BY datetime(updated_at) DESC, id DESC
            """,
                        (game_type, game_type),
        ).fetchall()

        games = []
        for row in rows:
            game = _normalize_game_title(conn, dict(row))
            # Parse grid_json
            try:
                parsed_grid = json.loads(game.get("grid_json", "[]"))
            except json.JSONDecodeError:
                parsed_grid = [["" for _ in range(9)] for _ in range(9)]
            
            if not isinstance(parsed_grid, list):
                parsed_grid = [["" for _ in range(9)] for _ in range(9)]
            
            # Normalize grid to 9x9
            normalized_grid = [["" for _ in range(9)] for _ in range(9)]
            for r in range(min(9, len(parsed_grid))):
                row = parsed_grid[r] if isinstance(parsed_grid[r], list) else []
                for c in range(min(9, len(row))):
                    normalized_grid[r][c] = str(row[c]).strip() if row[c] else ""
            
            game["grid"] = normalized_grid
            games.append(game)

    return games


def delete_game(database_path: str, game_id: int) -> None:
    with get_connection(database_path) as conn:
        conn.execute("DELETE FROM sudoku_games WHERE id = ?", (game_id,))
        conn.commit()


def get_game_by_id(database_path: str, game_id: int) -> dict[str, Any] | None:
    with get_connection(database_path) as conn:
        row = conn.execute(
            """
            SELECT id, title, difficulty, game_type, grid_json, notes_json, elapsed_seconds,
                 source_game_id, session_number, save_count, solution_json,
                  datetime(created_at, 'localtime') AS created_at,
                  datetime(updated_at, 'localtime') AS updated_at,
                  datetime(completed_at, 'localtime') AS completed_at
            FROM sudoku_games
            WHERE id = ?
            """,
            (game_id,),
        ).fetchone()

    if row is None:
        return None

    game = _normalize_game_title(conn, dict(row))
    try:
        parsed_grid = json.loads(game["grid_json"])
    except json.JSONDecodeError:
        parsed_grid = [["" for _ in range(9)] for _ in range(9)]

    parsed_notes = _safe_load_json(game.get("notes_json"), _empty_notes())
    parsed_solution = _safe_load_json(game.get("solution_json"), _empty_grid())

    if not isinstance(parsed_notes, list):
        parsed_notes = _empty_notes()

    normalized_notes = _empty_notes()
    for r in range(min(9, len(parsed_notes))):
        row = parsed_notes[r] if isinstance(parsed_notes[r], list) else []
        for c in range(min(9, len(row))):
            normalized_notes[r][c] = str(row[c]).strip()

    game["grid"] = _normalize_grid(parsed_grid)
    game["notes"] = normalized_notes
    game["solution"] = _normalize_grid(parsed_solution)
    return game


def upsert_leaderboard_entry(
    database_path: str,
    ongoing_game_id: int,
    source_game_id: int | None,
    game_title: str,
    difficulty: str,
    elapsed_seconds: int,
) -> None:
    with get_connection(database_path) as conn:
        conn.execute(
            """
            INSERT INTO sudoku_leaderboard
                (ongoing_game_id, source_game_id, game_title, difficulty, elapsed_seconds)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(ongoing_game_id) DO UPDATE SET
                game_title = excluded.game_title,
                difficulty = excluded.difficulty,
                elapsed_seconds = CASE
                    WHEN excluded.elapsed_seconds < sudoku_leaderboard.elapsed_seconds
                    THEN excluded.elapsed_seconds
                    ELSE sudoku_leaderboard.elapsed_seconds
                END
            """,
            (ongoing_game_id, source_game_id, game_title, difficulty, elapsed_seconds),
        )
        conn.commit()


def list_leaderboard(database_path: str, limit: int = 10) -> list[dict[str, Any]]:
    with get_connection(database_path) as conn:
        rows = conn.execute(
            """
            SELECT id, ongoing_game_id, source_game_id, game_title, difficulty,
                   elapsed_seconds,
                   datetime(completed_at, 'localtime') AS completed_at
            FROM sudoku_leaderboard
            ORDER BY elapsed_seconds ASC, datetime(completed_at) ASC, id ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        if rows:
            return [dict(row) for row in rows]

        # Fallback: derive leaderboard from completed ongoing games if leaderboard table is empty.
        fallback_rows = conn.execute(
            """
            SELECT id,
                   id AS ongoing_game_id,
                   source_game_id,
                   title AS game_title,
                   difficulty,
                   elapsed_seconds,
                   datetime(completed_at, 'localtime') AS completed_at
            FROM sudoku_games
            WHERE game_type = 'ongoing' AND completed_at IS NOT NULL
            ORDER BY elapsed_seconds ASC, datetime(completed_at) ASC, id ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        return [dict(row) for row in fallback_rows]


def mark_game_completed(database_path: str, game_id: int) -> None:
    with get_connection(database_path) as conn:
        conn.execute(
            """
            UPDATE sudoku_games
            SET completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (game_id,),
        )
        conn.commit()


def list_completed_games(database_path: str, limit: int = 200) -> list[dict[str, Any]]:
    with get_connection(database_path) as conn:
        rows = conn.execute(
            """
            SELECT id, title, difficulty, game_type, grid_json, notes_json, elapsed_seconds,
                 source_game_id, session_number, save_count, solution_json,
                   datetime(created_at, 'localtime') AS created_at,
                   datetime(updated_at, 'localtime') AS updated_at,
                   datetime(completed_at, 'localtime') AS completed_at
            FROM sudoku_games
            WHERE game_type = 'ongoing' AND completed_at IS NOT NULL
            ORDER BY datetime(completed_at) DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        games = []
        for row in rows:
            game = _normalize_game_title(conn, dict(row))
            try:
                parsed_grid = json.loads(game.get("grid_json", "[]"))
            except json.JSONDecodeError:
                parsed_grid = [["" for _ in range(9)] for _ in range(9)]

            if not isinstance(parsed_grid, list):
                parsed_grid = [["" for _ in range(9)] for _ in range(9)]

            normalized_grid = [["" for _ in range(9)] for _ in range(9)]
            for r in range(min(9, len(parsed_grid))):
                row_data = parsed_grid[r] if isinstance(parsed_grid[r], list) else []
                for c in range(min(9, len(row_data))):
                    normalized_grid[r][c] = str(row_data[c]).strip() if row_data[c] else ""

            game["grid"] = normalized_grid
            games.append(game)

        return games


def record_game_changes(
    database_path: str,
    ongoing_game_id: int,
    changes: list[dict[str, Any]],
) -> None:
    if not changes:
        return

    rows = [
        (
            ongoing_game_id,
            int(change["row_idx"]),
            int(change["col_idx"]),
            str(change.get("old_value", "")),
            str(change.get("new_value", "")),
            str(change.get("old_notes", "")),
            str(change.get("new_notes", "")),
        )
        for change in changes
    ]

    with get_connection(database_path) as conn:
        conn.executemany(
            """
            INSERT INTO sudoku_game_changes
                (ongoing_game_id, row_idx, col_idx, old_value, new_value, old_notes, new_notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()


def list_game_changes(database_path: str, ongoing_game_id: int) -> list[dict[str, Any]]:
    with get_connection(database_path) as conn:
        rows = conn.execute(
            """
            SELECT id,
                   ongoing_game_id,
                   row_idx,
                   col_idx,
                   old_value,
                   new_value,
                   old_notes,
                   new_notes,
                   datetime(changed_at, 'localtime') AS changed_at
            FROM sudoku_game_changes
            WHERE ongoing_game_id = ?
            ORDER BY id ASC
            """,
            (ongoing_game_id,),
        ).fetchall()
        return [dict(row) for row in rows]


def record_game_inputs(
    database_path: str,
    ongoing_game_id: int,
    input_entries: list[dict[str, Any]],
) -> None:
    if not input_entries:
        return

    rows: list[tuple[int, int, int, int, str, str, str | None, int, str | None]] = []
    for entry in input_entries:
        value = str(entry.get("value", "")).strip()
        if value not in {"1", "2", "3", "4", "5", "6", "7", "8", "9"}:
            continue

        seq_no = int(entry.get("seq_no", 0))
        if seq_no <= 0:
            continue

        row_idx = int(entry.get("row_idx", -1))
        col_idx = int(entry.get("col_idx", -1))
        if row_idx < 0 or row_idx > 8 or col_idx < 0 or col_idx > 8:
            continue

        elapsed_seconds = int(entry.get("elapsed_seconds", 0) or 0)
        entered_at_raw = entry.get("entered_at")
        entered_at = str(entered_at_raw).strip() if entered_at_raw else None
        input_kind = str(entry.get("input_kind", "manual") or "manual").strip().lower()
        if input_kind not in {"manual", "logic"}:
            input_kind = "manual"
        action_name_raw = str(entry.get("action_name", "") or "").strip()
        action_name = action_name_raw[:120] if action_name_raw else None

        rows.append(
            (
                ongoing_game_id,
                seq_no,
                row_idx,
                col_idx,
                value,
                input_kind,
                action_name,
                elapsed_seconds,
                entered_at,
            )
        )

    if not rows:
        return

    with get_connection(database_path) as conn:
        conn.executemany(
            """
            INSERT INTO sudoku_game_inputs
                (ongoing_game_id, seq_no, row_idx, col_idx, value, input_kind, action_name, elapsed_seconds, entered_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ongoing_game_id, seq_no) DO NOTHING
            """,
            rows,
        )
        conn.commit()


def list_game_inputs(database_path: str, ongoing_game_id: int) -> list[dict[str, Any]]:
    with get_connection(database_path) as conn:
        rows = conn.execute(
            """
            SELECT id,
                   ongoing_game_id,
                   seq_no,
                   row_idx,
                   col_idx,
                   value,
                     input_kind,
                     action_name,
                   elapsed_seconds,
                   entered_at,
                   datetime(created_at, 'localtime') AS created_at
            FROM sudoku_game_inputs
            WHERE ongoing_game_id = ?
            ORDER BY seq_no ASC, id ASC
            """,
            (ongoing_game_id,),
        ).fetchall()
        return [dict(row) for row in rows]
