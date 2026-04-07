from flask import Blueprint, current_app, flash, redirect, render_template, request, url_for
from markupsafe import Markup
import json

from .storage import (
    delete_game,
    find_base_game_id_by_grid,
    get_game_by_id,
    list_completed_games,
    list_game_inputs,
    list_games,
    list_leaderboard,
    mark_game_completed,
    record_game_inputs,
    save_game,
    update_game,
    upsert_leaderboard_entry,
)

bp = Blueprint("main", __name__)


def _validate_sudoku(grid: list[list[str]]) -> str | None:
    """Return an error message if the grid has conflicts, otherwise None."""
    for r in range(9):
        seen: set[str] = set()
        for c in range(9):
            v = grid[r][c]
            if v:
                if v in seen:
                    return f"Rad {r + 1} innehåller siffran {v} mer än en gång."
                seen.add(v)

    for c in range(9):
        seen = set()
        for r in range(9):
            v = grid[r][c]
            if v:
                if v in seen:
                    return f"Kolumn {c + 1} innehåller siffran {v} mer än en gång."
                seen.add(v)

    for box_r in range(3):
        for box_c in range(3):
            seen = set()
            for r in range(box_r * 3, box_r * 3 + 3):
                for c in range(box_c * 3, box_c * 3 + 3):
                    v = grid[r][c]
                    if v:
                        if v in seen:
                            return f"Rutan ({box_r * 3 + 1}–{box_r * 3 + 3}, {box_c * 3 + 1}–{box_c * 3 + 3}) innehåller siffran {v} mer än en gång."
                        seen.add(v)

    return None


def _sanitize_notes_value(value: str) -> str:
    digits = sorted({ch for ch in value if ch in "123456789"})
    return "".join(digits)


def _is_completed_sudoku(grid: list[list[str]]) -> bool:
    allowed = {"1", "2", "3", "4", "5", "6", "7", "8", "9"}
    for row in grid:
        for value in row:
            if value not in allowed:
                return False
    return _validate_sudoku(grid) is None


def _build_grid_differences(
    old_grid: list[list[str]],
    new_grid: list[list[str]],
    old_notes: list[list[str]],
    new_notes: list[list[str]],
) -> list[dict[str, str | int]]:
    changes: list[dict[str, str | int]] = []
    for r in range(9):
        for c in range(9):
            prev_value = old_grid[r][c] if r < len(old_grid) and c < len(old_grid[r]) else ""
            curr_value = new_grid[r][c] if r < len(new_grid) and c < len(new_grid[r]) else ""
            prev_notes = old_notes[r][c] if r < len(old_notes) and c < len(old_notes[r]) else ""
            curr_notes = new_notes[r][c] if r < len(new_notes) and c < len(new_notes[r]) else ""

            if prev_value == curr_value and prev_notes == curr_notes:
                continue

            changes.append(
                {
                    "row_idx": r,
                    "col_idx": c,
                    "old_value": prev_value,
                    "new_value": curr_value,
                    "old_notes": prev_notes,
                    "new_notes": curr_notes,
                }
            )

    return changes


def _build_fixed_cells_from_grid(raw_grid: list[list[str]] | None) -> list[list[bool]]:
    fixed = [[False for _ in range(9)] for _ in range(9)]
    if not isinstance(raw_grid, list):
        return fixed

    for r in range(min(9, len(raw_grid))):
        row = raw_grid[r] if isinstance(raw_grid[r], list) else []
        for c in range(min(9, len(row))):
            value = str(row[c]).strip() if row[c] is not None else ""
            fixed[r][c] = value in {"1", "2", "3", "4", "5", "6", "7", "8", "9"}

    return fixed


def _parse_input_log_json(raw_value: str) -> list[dict[str, int | str]]:
    if not raw_value.strip():
        return []
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []

    entries: list[dict[str, int | str]] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        value = str(item.get("value", "")).strip()
        if value not in {"1", "2", "3", "4", "5", "6", "7", "8", "9"}:
            continue
        try:
            seq_no = int(item.get("seq_no", 0))
            row_idx = int(item.get("row_idx", -1))
            col_idx = int(item.get("col_idx", -1))
            elapsed_seconds = int(item.get("elapsed_seconds", 0) or 0)
        except (TypeError, ValueError):
            continue
        if seq_no <= 0 or not (0 <= row_idx <= 8) or not (0 <= col_idx <= 8):
            continue
        entry: dict[str, int | str] = {
            "seq_no": seq_no,
            "row_idx": row_idx,
            "col_idx": col_idx,
            "value": value,
            "elapsed_seconds": max(0, elapsed_seconds),
        }
        input_kind = str(item.get("input_kind", "manual") or "manual").strip().lower()
        if input_kind not in {"manual", "logic"}:
            input_kind = "manual"
        entry["input_kind"] = input_kind

        action_name = str(item.get("action_name", "") or "").strip()
        if input_kind == "logic" and action_name:
            entry["action_name"] = action_name[:120]

        entered_at = str(item.get("entered_at", "")).strip()
        if entered_at:
            entry["entered_at"] = entered_at
        entries.append(entry)

    return entries


@bp.route("/")
def index():
    ongoing = list_games(current_app.config["DATABASE_PATH"], game_type="ongoing")
    leaderboard = list_leaderboard(current_app.config["DATABASE_PATH"], limit=10)
    return render_template("index.html", ongoing_games=ongoing[:10], leaderboard=leaderboard)


@bp.route("/leaderboard")
def leaderboard():
    leaderboard_rows = list_leaderboard(current_app.config["DATABASE_PATH"], limit=1000)
    requested_difficulty = str(request.args.get("difficulty", "")).strip().lower()
    difficulty_order = ["latt", "medel", "svar", "expert"]
    difficulty_labels = {
        "latt": "Lätt",
        "medel": "Medel",
        "svar": "Svår",
        "expert": "Expert",
    }

    def normalize_difficulty(value: str) -> str:
        return value.replace("å", "a").replace("ä", "a").replace("ö", "o")

    requested_key = normalize_difficulty(requested_difficulty) if requested_difficulty else ""

    grouped_map: dict[str, list[dict[str, object]]] = {}
    for row in leaderboard_rows:
        raw = str(row.get("difficulty", "")).strip().lower()
        key = normalize_difficulty(raw)
        if not key:
            key = "okand"
        grouped_map.setdefault(key, []).append(row)

    grouped_leaderboard = []
    if requested_key:
        rows = grouped_map.get(requested_key, [])
        label = difficulty_labels.get(requested_key, requested_key.capitalize())
        if rows:
            grouped_leaderboard.append(
                {
                    "key": requested_key,
                    "label": label,
                    "rows": rows,
                    "full_url": None,
                    "is_full": True,
                }
            )
    else:
        for key in difficulty_order:
            rows = grouped_map.pop(key, [])
            if rows:
                grouped_leaderboard.append(
                    {
                        "key": key,
                        "label": difficulty_labels[key],
                        "rows": rows[:5],
                        "full_url": url_for("main.leaderboard", difficulty=key),
                        "is_full": False,
                        "total_count": len(rows),
                    }
                )

        for key in sorted(grouped_map.keys()):
            rows = grouped_map[key]
            if not rows:
                continue
            grouped_leaderboard.append(
                {
                    "key": key,
                    "label": key.capitalize(),
                    "rows": rows[:5],
                    "full_url": url_for("main.leaderboard", difficulty=key),
                    "is_full": False,
                    "total_count": len(rows),
                }
            )

    return render_template(
        "leaderboard.html",
        grouped_leaderboard=grouped_leaderboard,
        leaderboard=leaderboard_rows,
        requested_difficulty_key=requested_key,
        requested_difficulty_label=difficulty_labels.get(requested_key, requested_key.capitalize() if requested_key else ""),
    )


@bp.route("/builder")
def builder():
    game_id = request.args.get("game_id", type=int)
    grid_values = [["" for _ in range(9)] for _ in range(9)]
    note_values = [["" for _ in range(9)] for _ in range(9)]
    fixed_cells = [[False for _ in range(9)] for _ in range(9)]
    solution_grid = [["" for _ in range(9)] for _ in range(9)]
    selected_difficulty = ""
    loaded_game = None
    elapsed_seconds = 0
    input_log: list[dict[str, int | str]] = []

    if game_id is not None:
        loaded_game = get_game_by_id(current_app.config["DATABASE_PATH"], game_id)
        if loaded_game is None:
            flash(f"Spel med ID {game_id} hittades inte.", "error")
            return redirect(url_for("main.games"))

        # If loading a base game, immediately create an ongoing copy and redirect to it.
        if loaded_game.get("game_type") == "base":
            raw_grid = loaded_game.get("grid", [])
            new_id = save_game(
                current_app.config["DATABASE_PATH"],
                difficulty=loaded_game["difficulty"],
                grid=raw_grid,
                notes=[["" for _ in range(9)] for _ in range(9)],
                game_type="ongoing",
                elapsed_seconds=0,
                source_game_id=game_id,
            )
            return redirect(url_for("main.builder", game_id=new_id))

        raw_grid = loaded_game.get("grid", [])
        raw_notes = loaded_game.get("notes", [])
        if isinstance(raw_grid, list):
            for r in range(min(9, len(raw_grid))):
                row = raw_grid[r] if isinstance(raw_grid[r], list) else []
                for c in range(min(9, len(row))):
                    value = str(row[c]).strip() if row[c] is not None else ""
                    grid_values[r][c] = value if value in {"1", "2", "3", "4", "5", "6", "7", "8", "9"} else ""
        if isinstance(raw_notes, list):
            for r in range(min(9, len(raw_notes))):
                row = raw_notes[r] if isinstance(raw_notes[r], list) else []
                for c in range(min(9, len(row))):
                    note_values[r][c] = _sanitize_notes_value(str(row[c]).strip())

        selected_difficulty = loaded_game.get("difficulty", "")
        elapsed_seconds = loaded_game.get("elapsed_seconds", 0) or 0

        if loaded_game.get("game_type") == "ongoing" and loaded_game.get("source_game_id"):
            source_game = get_game_by_id(
                current_app.config["DATABASE_PATH"], int(loaded_game["source_game_id"])
            )
            source_grid = source_game.get("grid", []) if source_game else []
            fixed_cells = _build_fixed_cells_from_grid(source_grid)
            source_solution = source_game.get("solution", []) if source_game else []
            if isinstance(source_solution, list):
                solution_grid = source_solution
            input_log = list_game_inputs(current_app.config["DATABASE_PATH"], int(loaded_game["id"]))
        elif loaded_game.get("game_type") == "ongoing":
            own_solution = loaded_game.get("solution", [])
            if isinstance(own_solution, list):
                solution_grid = own_solution
            input_log = list_game_inputs(current_app.config["DATABASE_PATH"], int(loaded_game["id"]))

    return render_template(
        "builder.html",
        grid_values=grid_values,
        note_values=note_values,
        fixed_cells=fixed_cells,
        solution_grid=solution_grid,
        input_log=input_log,
        selected_difficulty=selected_difficulty,
        loaded_game=loaded_game,
        elapsed_seconds=elapsed_seconds,
    )


@bp.route("/games")
def games():
    games_data = list_games(current_app.config["DATABASE_PATH"], game_type="base")
    difficulty_order = ["expert", "svar", "medel", "latt"]
    difficulty_labels = {
        "expert": "Expert",
        "svar": "Svår",
        "medel": "Medel",
        "latt": "Lätt",
    }

    grouped_map: dict[str, list[dict[str, object]]] = {}
    for game in games_data:
        key = str(game.get("difficulty", "")).strip().lower()
        if not key:
            key = "okand"
        grouped_map.setdefault(key, []).append(game)

    grouped_games: list[dict[str, object]] = []
    for key in difficulty_order:
        rows = grouped_map.pop(key, [])
        if rows:
            grouped_games.append({
                "key": key,
                "label": difficulty_labels[key],
                "rows": rows,
            })

    for key in sorted(grouped_map.keys()):
        rows = grouped_map[key]
        if not rows:
            continue
        grouped_games.append({
            "key": key,
            "label": key.capitalize(),
            "rows": rows,
        })

    return render_template("games.html", games=games_data, grouped_games=grouped_games)


@bp.route("/ongoing/delete/<int:game_id>", methods=["POST"])
def delete_ongoing_game(game_id: int):
    from .storage import delete_game as storage_delete
    storage_delete(current_app.config["DATABASE_PATH"], game_id)
    flash(f"Pågående spel raderades.", "success")
    return redirect(url_for("main.ongoing_games"))


@bp.route("/ongoing")
def ongoing_games():
    games_data = list_games(current_app.config["DATABASE_PATH"], game_type="ongoing")
    return render_template("ongoing_games.html", games=games_data)


@bp.route("/completed")
def completed_games():
    games_data = list_completed_games(current_app.config["DATABASE_PATH"], limit=200)
    return render_template("completed_games.html", games=games_data)


@bp.route("/completed/delete/<int:game_id>", methods=["POST"])
def delete_completed_game(game_id: int):
    game = get_game_by_id(current_app.config["DATABASE_PATH"], game_id)
    if game is None or game.get("game_type") != "ongoing" or not game.get("completed_at"):
        flash("Avslutad omgång hittades inte.", "error")
        return redirect(url_for("main.completed_games"))

    delete_game(current_app.config["DATABASE_PATH"], game_id)
    flash("Avslutad omgång raderades.", "success")
    return redirect(url_for("main.completed_games"))


@bp.route("/completed/<int:game_id>")
def completed_game_detail(game_id: int):
    game = get_game_by_id(current_app.config["DATABASE_PATH"], game_id)
    if game is None or game.get("game_type") != "ongoing" or not game.get("completed_at"):
        flash("Avslutat spel hittades inte.", "error")
        return redirect(url_for("main.completed_games"))

    input_entries = list_game_inputs(current_app.config["DATABASE_PATH"], game_id)
    return render_template("completed_game_detail.html", game=game, input_entries=input_entries)


@bp.route("/games/save", methods=["POST"])
def save_game_route():
    source_game_id = request.form.get("source_game_id", "").strip()
    source_game_type = request.form.get("source_game_type", "").strip().lower()
    save_action = request.form.get("save_action", "save").strip().lower()
    difficulty = request.form.get("difficulty", "").strip().lower()
    allowed_difficulty = {"latt", "medel", "svar", "expert"}

    try:
        elapsed_seconds = int(request.form.get("elapsed_seconds", "0"))
    except ValueError:
        elapsed_seconds = 0

    input_log_entries = _parse_input_log_json(request.form.get("input_log_json", ""))

    def _render_error(message: str, category: str = "error"):
        loaded_game = None
        fixed_cells = [[False for _ in range(9)] for _ in range(9)]
        solution_grid = [["" for _ in range(9)] for _ in range(9)]
        if source_game_id.isdigit():
            loaded_game = get_game_by_id(current_app.config["DATABASE_PATH"], int(source_game_id))
            if loaded_game and loaded_game.get("game_type") == "ongoing" and loaded_game.get("source_game_id"):
                source_game = get_game_by_id(
                    current_app.config["DATABASE_PATH"], int(loaded_game["source_game_id"])
                )
                source_grid = source_game.get("grid", []) if source_game else []
                fixed_cells = _build_fixed_cells_from_grid(source_grid)
                source_solution = source_game.get("solution", []) if source_game else []
                if isinstance(source_solution, list):
                    solution_grid = source_solution
            elif loaded_game and loaded_game.get("game_type") == "ongoing":
                own_solution = loaded_game.get("solution", [])
                if isinstance(own_solution, list):
                    solution_grid = own_solution
        flash(message, category)
        return render_template(
            "builder.html",
            grid_values=grid if grid else [["" for _ in range(9)] for _ in range(9)],
            note_values=notes if notes else [["" for _ in range(9)] for _ in range(9)],
            fixed_cells=fixed_cells,
            solution_grid=solution_grid,
            input_log=input_log_entries,
            selected_difficulty=difficulty,
            loaded_game=loaded_game,
            elapsed_seconds=elapsed_seconds,
        )

    grid: list[list[str]] = []
    notes: list[list[str]] = []
    for r in range(9):
        row: list[str] = []
        note_row: list[str] = []
        for c in range(9):
            value = request.form.get(f"cell_{r}_{c}", "").strip()
            note_value = _sanitize_notes_value(request.form.get(f"note_{r}_{c}", "").strip())
            if value and value not in {"1", "2", "3", "4", "5", "6", "7", "8", "9"}:
                grid.append(row)
                notes.append(note_row)
                return _render_error("Alla ifyllda rutor maste vara siffror 1-9.")
            row.append(value)
            note_row.append(note_value if not value else "")
        grid.append(row)
        notes.append(note_row)

    if difficulty not in allowed_difficulty:
        return _render_error("Du maste valja en giltig svarighetsgrad.")

    conflict = _validate_sudoku(grid)
    if conflict:
        return _render_error(conflict)

    if source_game_id.isdigit() and source_game_type == "ongoing":
        ongoing_id = int(source_game_id)
        ongoing_game = get_game_by_id(current_app.config["DATABASE_PATH"], ongoing_id)
        if ongoing_game is None:
            flash(f"Pågående spel {ongoing_id} hittades inte.", "error")
            return redirect(url_for("main.ongoing_games"))

        source_id = ongoing_game.get("source_game_id")
        if source_id:
            source_game = get_game_by_id(current_app.config["DATABASE_PATH"], int(source_id))
            source_grid = source_game.get("grid", []) if source_game else []
            fixed_cells = _build_fixed_cells_from_grid(source_grid)
            for r in range(9):
                for c in range(9):
                    if not fixed_cells[r][c]:
                        continue
                    required_value = str(source_grid[r][c]).strip()
                    if grid[r][c] != required_value:
                        return _render_error("Grundspelets ifyllda värden kan inte ändras.")

        if save_action == "save_as":
            source_ref = int(source_id) if source_id else ongoing_id
            new_game_id = save_game(
                current_app.config["DATABASE_PATH"],
                difficulty=difficulty,
                grid=grid,
                notes=notes,
                game_type="ongoing",
                elapsed_seconds=elapsed_seconds,
                source_game_id=source_ref,
                include_version_suffix=True,
            )
            record_game_inputs(
                current_app.config["DATABASE_PATH"],
                new_game_id,
                input_log_entries,
            )

            if _is_completed_sudoku(grid):
                mark_game_completed(current_app.config["DATABASE_PATH"], new_game_id)
                new_game = get_game_by_id(current_app.config["DATABASE_PATH"], new_game_id)
                new_name = str(new_game.get("title", new_game_id)) if new_game else str(new_game_id)
                new_prefix = new_name.split(" (", 1)[0]
                upsert_leaderboard_entry(
                    current_app.config["DATABASE_PATH"],
                    ongoing_game_id=new_game_id,
                    source_game_id=source_ref,
                    game_title=new_prefix,
                    difficulty=difficulty,
                    elapsed_seconds=elapsed_seconds,
                )
                flash(
                    f"Spara som skapade {new_name}. Spelet är löst och tillagt i topplistan.",
                    "success",
                )
                return redirect(url_for("main.completed_game_detail", game_id=new_game_id))

            created_game = get_game_by_id(current_app.config["DATABASE_PATH"], new_game_id)
            created_name = str(created_game.get("title", new_game_id)) if created_game else str(new_game_id)
            flash(f"Spara som skapade {created_name}.", "success")
            return redirect(url_for("main.builder", game_id=new_game_id))

        was_updated = update_game(
            current_app.config["DATABASE_PATH"],
            game_id=ongoing_id,
            difficulty=difficulty,
            grid=grid,
            notes=notes,
            elapsed_seconds=elapsed_seconds,
        )
        if not was_updated:
            flash(f"Pågående spel {ongoing_id} hittades inte.", "error")
            return redirect(url_for("main.ongoing_games"))

        record_game_inputs(
            current_app.config["DATABASE_PATH"],
            ongoing_id,
            input_log_entries,
        )

        updated_game = get_game_by_id(current_app.config["DATABASE_PATH"], ongoing_id)
        ongoing_name = str(updated_game.get("title", ongoing_id)) if updated_game else str(ongoing_id)
        ongoing_prefix = ongoing_name.split(" (", 1)[0]

        if _is_completed_sudoku(grid):
            source_id = updated_game.get("source_game_id") if updated_game else None
            mark_game_completed(current_app.config["DATABASE_PATH"], ongoing_id)
            upsert_leaderboard_entry(
                current_app.config["DATABASE_PATH"],
                ongoing_game_id=ongoing_id,
                source_game_id=int(source_id) if source_id else None,
                game_title=ongoing_prefix,
                difficulty=difficulty,
                elapsed_seconds=elapsed_seconds,
            )
            return redirect(url_for("main.completed_game_detail", game_id=ongoing_id))

        flash(f"Pågående spel {ongoing_prefix} uppdaterades.", "success")
        return redirect(url_for("main.ongoing_games"))

    game_type = "ongoing" if source_game_id.isdigit() else "base"
    if game_type == "base":
        existing_base_id = find_base_game_id_by_grid(
            current_app.config["DATABASE_PATH"],
            grid,
        )
        if existing_base_id is not None:
            existing_game_url = url_for("main.builder", game_id=existing_base_id)
            return _render_error(
                Markup(
                    f"Detta grundspel finns redan sparat som spel "
                    f"<a href=\"{existing_game_url}\">{existing_base_id}</a>."
                ),
                category="duplicate_base",
            )

    try:
        new_game_id = save_game(
            current_app.config["DATABASE_PATH"],
            difficulty=difficulty,
            grid=grid,
            notes=notes,
            game_type=game_type,
            elapsed_seconds=elapsed_seconds,
        )
    except ValueError as err:
        return _render_error(str(err))

    if game_type == "ongoing":
        flash(f"Pågående spel sparat som {new_game_id}.", "success")
        return redirect(url_for("main.ongoing_games"))

    flash(f"Spel sparat som {new_game_id}.", "success")
    return redirect(url_for("main.games"))
