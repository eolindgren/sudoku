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
            count = conn.execute(
                "SELECT COUNT(*) FROM sudoku_games WHERE game_type = 'base'"
            ).fetchone()[0]
            title = str(count + 1)
            cursor = conn.execute(
                """
                INSERT INTO sudoku_games
                    (title, difficulty, game_type, grid_json, notes_json, elapsed_seconds,
                     source_game_id, session_number, save_count, updated_at, completed_at)
                VALUES (?, ?, ?, ?, ?, ?, NULL, 0, 0, CURRENT_TIMESTAMP, NULL)
                """,
                (title, difficulty, game_type, json.dumps(grid), json.dumps(notes), elapsed_seconds),
            )
        else:
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
                     source_game_id, session_number, save_count, updated_at, completed_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP, NULL)
                """,
                (title, difficulty, game_type, json.dumps(grid), json.dumps(notes), elapsed_seconds,
                 source_game_id, session_number),
            )
        conn.commit()
        return int(cursor.lastrowid)


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
                 source_game_id, session_number, save_count,
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
                 source_game_id, session_number, save_count,
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

    try:
        parsed_notes = json.loads(game.get("notes_json", "[]"))
    except json.JSONDecodeError:
        parsed_notes = _empty_notes()

    if not isinstance(parsed_notes, list):
        parsed_notes = _empty_notes()

    normalized_notes = _empty_notes()
    for r in range(min(9, len(parsed_notes))):
        row = parsed_notes[r] if isinstance(parsed_notes[r], list) else []
        for c in range(min(9, len(row))):
            normalized_notes[r][c] = str(row[c]).strip()

    game["grid"] = parsed_grid
    game["notes"] = normalized_notes
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

        return [dict(row) for row in rows]


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
                   source_game_id, session_number, save_count,
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
