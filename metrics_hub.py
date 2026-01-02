#!/usr/bin/env python3
"""
Minimal Hub Metrics Tap (Jetson)

What it does:
- t=0: publish hub/command "1"
- subscribe to robot topics: 00..03 + digits 1..6 (via '#' + filter)
- end run on first target/alert (digit '5')
- prompt user: algorithm id (int), message error (0..1 float)
- export 2 CSV files (append mode; one set per algorithm):
  <alg>_sys.csv
  <alg>_robots.csv

Each trial appends:
- 1 row to <alg>_sys.csv
- 1 row per robot to <alg>_robots.csv

Coordinate payload expected: "x,y-" (hyphen optional); we strip '-'
CSV coordinate storage uses "x/y" to avoid Excel splitting columns.

This script folds the old "locations" file into:
- System metrics: a single field containing all clue coordinates as a list of tuples.
- Robot metrics: per-robot clue list, per-robot position at first clue, and a per-robot
  flag for whether that robot found the target.

Clue/target messages are still printed live to the terminal during the trial, but all
CSV fields are written only once at the end of the trial.
"""

import csv
import os
import re
import time
from typing import Dict, List, Optional, Tuple

import paho.mqtt.client as mqtt

# ---- Config ----
BROKER_HOST = "192.168.1.10"
BROKER_PORT = 1883

HUB_COMMAND_TOPIC = "hub/command"
START_PAYLOAD = "1"

ROBOT_IDS = ["00", "01", "02", "03"]
VALID_DIGITS = set("123456")

OUT_DIR = "./hub_logs"


# ---- Helpers ----
def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def csv_append(path: str, header: List[str], rows: List[List[object]]) -> None:
    """Append rows to a CSV, writing header once if the file is new/empty."""
    write_header = (not os.path.exists(path)) or (os.path.getsize(path) == 0)
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(header)
        w.writerows(rows)


def now_s() -> float:
    return time.time()


def parse_robot_topic(topic: str) -> Optional[Tuple[str, str]]:
    # robot topics like "021" => rid="02", dig="1"
    if len(topic) != 3:
        return None
    rid, dig = topic[:2], topic[2]
    if rid in ROBOT_IDS and dig in VALID_DIGITS:
        return rid, dig
    return None


def parse_coord(payload: str) -> Optional[Tuple[int, int]]:
    # accepts "x,y-" or "x,y"
    s = payload.strip()
    if s.endswith("-"):
        s = s[:-1].strip()
    m = re.fullmatch(r"\s*(-?\d+)\s*,\s*(-?\d+)\s*", s)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def cell_csv(x: int, y: int) -> str:
    # safe for CSV/Excel (no commas)
    return f"{x}/{y}"


def prompt_name(msg: str) -> str:
    """Prompt for a short algorithm ID like VP/DP/AC (2 characters)."""
    while True:
        s = input(msg).strip().upper()
        if len(s) == 2 and s.isalnum():
            return s
        print("Enter a 2-character algorithm ID (example: VP, DP, AC).")


def prompt_float_0_1(msg: str) -> float:
    while True:
        s = input(msg).strip()
        try:
            v = float(s)
            if 0.0 <= v <= 1.0:
                return v
        except ValueError:
            pass
        print("Enter a number between 0 and 1 (example: 0.15).")


# ---- State (simple dicts) ----
def make_robot_state() -> Dict:
    return {
        "last_pos": None,               # (x,y)
        "steps_total": 0,
        "steps_preclue": 0,
        "steps_postclue": 0,
        "visited": set(),               # set[(x,y)]
        "revisits": 0,
        "clues": [],                    # list[(x,y)] found by this robot
        "found_target": "n",            # 'y' if this robot reported the target
        "msg_by_digit": {d: 0 for d in "123456"},
    }


def main() -> None:
    ensure_dir(OUT_DIR)

    # Trial state
    t0 = None  # set right before publishing start
    t_first_clue = None
    first_clue_loc = None
    pos_at_first_clue = None  # dict rid -> (x,y)
    clue_locs: List[Tuple[int, int]] = []

    t_end = None
    end_reporter = None
    end_loc_reported = None

    robots: Dict[str, Dict] = {rid: make_robot_state() for rid in ROBOT_IDS}

    team_visit_counts: Dict[Tuple[int, int], int] = {}
    msg_total_system = 0
    msg_total_by_digit = {d: 0 for d in "123456"}

    done_flag = False  # mutable closure flag
    drain_time = None

    def mark_team_visit(cell: Tuple[int, int]) -> None:
        team_visit_counts[cell] = team_visit_counts.get(cell, 0) + 1

    def handle_position(rid: str, cell: Tuple[int, int], t_rel: float) -> None:
        rs = robots[rid]

        if rs["last_pos"] is None:
            rs["last_pos"] = cell
            # count starting cell as visited
            if cell in rs["visited"]:
                rs["revisits"] += 1
            else:
                rs["visited"].add(cell)
            mark_team_visit(cell)
            return

        if cell == rs["last_pos"]:
            return

        # step
        rs["last_pos"] = cell
        rs["steps_total"] += 1

        if t_first_clue is None:
            rs["steps_preclue"] += 1
        else:
            if t_rel < t_first_clue:
                rs["steps_preclue"] += 1
            else:
                rs["steps_postclue"] += 1

        if cell in rs["visited"]:
            rs["revisits"] += 1
        else:
            rs["visited"].add(cell)

        mark_team_visit(cell)

    def handle_clue(rid: str, cell: Tuple[int, int], t_rel: float) -> None:
        nonlocal t_first_clue, first_clue_loc, pos_at_first_clue
        clue_locs.append(cell)
        robots[rid]["clues"].append(cell)

        # Live terminal feedback during trial (no timestamps written to CSV)
        print(f"[CLUE] {rid} @ {cell}")

        if t_first_clue is None:
            t_first_clue = t_rel
            first_clue_loc = cell
            # snapshot robot positions at first clue
            pos_at_first_clue = {r: robots[r]["last_pos"] for r in ROBOT_IDS}
            print(f"[RUN] First clue t={t_rel:.3f}s loc={cell} from {rid}")

    def handle_target(rid: str, cell: Tuple[int, int], t_rel: float) -> None:
        nonlocal t_end, end_reporter, end_loc_reported, done_flag, drain_time
        if t_end is not None:
            return
        t_end = t_rel
        end_reporter = rid
        end_loc_reported = cell
        robots[rid]["found_target"] = "y"
        print(f"[RUN] End t={t_rel:.3f}s loc={cell} from {rid}")
        done_flag = True
        drain_time = time.time()

    # MQTT callbacks
    def on_connect(client, userdata, flags, rc):
        if rc != 0:
            raise RuntimeError(f"MQTT connect failed rc={rc}")
        client.subscribe("#")  # simplest; filter by topic format in on_message

    def on_message(client, userdata, msg):
        nonlocal msg_total_system

        parsed = parse_robot_topic(msg.topic)
        if not parsed:
            return  # ignore hub/command and anything else

        rid, dig = parsed
        payload = msg.payload.decode(errors="ignore")

        # message counts
        msg_total_system += 1
        msg_total_by_digit[dig] += 1
        robots[rid]["msg_by_digit"][dig] += 1

        # only parse coords for 1/4/5
        if dig in {"1", "4", "5"}:
            cell = parse_coord(payload)
            if cell is None:
                return
        else:
            return  # only counting msgs for 2/3/6

        t_rel = now_s() - t0

        if dig == "1":
            handle_position(rid, cell, t_rel)
        elif dig == "4":
            handle_clue(rid, cell, t_rel)
        elif dig == "5":
            handle_target(rid, cell, t_rel)

    # Setup MQTT client
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER_HOST, BROKER_PORT, keepalive=10)

    # Start MQTT loop
    client.loop_start()

    # Start trial at t=0
    t0 = now_s()
    client.publish(HUB_COMMAND_TOPIC, START_PAYLOAD, qos=0, retain=False)
    print('[RUN] Published hub/command "1" (t=0.000s)')

    # Run until done
    try:
        while not done_flag:
            time.sleep(0.01)
        while abs(now_s()-drain_time) < 1.0:
            time.sleep(0.01)
    except KeyboardInterrupt:
        print("\n[RUN] Interrupted. No files written.")
        client.loop_stop()
        return

    client.loop_stop()

    if t_end is None or end_loc_reported is None:
        print("[ERR] No termination message (?5). No files written.")
        return

    # Prompts
    alg = prompt_name("Algorithm (VP,DP,AC): ")
    err = prompt_float_0_1("Message error (0..1, e.g. 0.15): ")
    target_loc = end_loc_reported

    # One set of files per algorithm; append one trial per run.
    sys_path = os.path.join(OUT_DIR, f"{alg}_sys.csv")
    robots_path = os.path.join(OUT_DIR, f"{alg}_robots.csv")

    # Fold "locations" into system + robot fields
    def fmt_tuple_list(cells: List[Tuple[int, int]]) -> str:
        # CSV writer will quote this; Excel will treat as a single string cell.
        return "[" + ",".join(f"({x},{y})" for (x, y) in cells) + "]"

    # Compute system metrics in your required order
    cf = "y" if t_first_clue is not None else "n"
    tt = float(t_end)  # total time
    tbc = float(t_first_clue) if t_first_clue is not None else tt
    tac = float(tt - tbc) if t_first_clue is not None else 0.0

    st = sum(robots[r]["steps_total"] for r in ROBOT_IDS)
    stbc = sum(robots[r]["steps_preclue"] for r in ROBOT_IDS)
    stac = sum(robots[r]["steps_postclue"] for r in ROBOT_IDS)

    uc = len(team_visit_counts)
    rv = sum(max(0, c - 1) for c in team_visit_counts.values())

    msg = msg_total_system
    m1, m2, m3, m4, m5, m6 = (msg_total_by_digit[d] for d in "123456")

    # Write system CSV
    sys_header = [
        "alg", "tgt", "err",
        "cf",
        "st", "tt",
        "stbc", "tbc",
        "stac", "tac",
        "uc", "rv",
        "cl",  # all clue locations, list of tuples
        "msg",
        "m1", "m2", "m3", "m4", "m5", "m6",
    ]
    sys_row = [
        alg, cell_csv(*target_loc), err,
        cf,
        st, round(tt, 6),
        stbc, round(tbc, 6),
        stac, round(tac, 6),
        uc, rv,
        fmt_tuple_list(clue_locs),
        msg,
        m1, m2, m3, m4, m5, m6,
    ]
    csv_append(sys_path, sys_header, [sys_row])

    # Write robots CSV
    robots_header = [
        "alg", "tgt", "err",
        "rid",
        "st", "stbc", "stac",
        "uc", "rv",
        "cl",      # clues found by this robot (list of tuples)
        "pfc",     # this robot's position when first clue happened (x/y)
        "tf",      # did this robot report the target? (y/n)
        "msg",
        "m1", "m2", "m3", "m4", "m5", "m6",
    ]
    robot_rows: List[List[object]] = []
    for rid in ROBOT_IDS:
        rs = robots[rid]
        pfc = ""
        if pos_at_first_clue is not None:
            p = pos_at_first_clue.get(rid)
            if p is not None:
                pfc = cell_csv(*p)
        robot_rows.append([
            alg, cell_csv(*target_loc), err,
            rid,
            rs["steps_total"], rs["steps_preclue"], rs["steps_postclue"],
            len(rs["visited"]), rs["revisits"],
            fmt_tuple_list(rs["clues"]),
            pfc,
            rs["found_target"],
            sum(rs["msg_by_digit"].values()),
            rs["msg_by_digit"]["1"], rs["msg_by_digit"]["2"], rs["msg_by_digit"]["3"],
            rs["msg_by_digit"]["4"], rs["msg_by_digit"]["5"], rs["msg_by_digit"]["6"],
        ])
    csv_append(robots_path, robots_header, robot_rows)

    print("\n[OK] Wrote:")
    print(" ", sys_path)
    print(" ", robots_path)


if __name__ == "__main__":
    main()


