# FortCore - High-performance PvP Core Plugin for Endstone
# Version 1.4 - Fixed for Endstone 0.10.18 (Minecraft 1.21.1)

from endstone.plugin import Plugin
from endstone.command import Command, CommandSender
from endstone.event import event_handler, EventPriority, PlayerJoinEvent, PlayerDeathEvent, PlayerQuitEvent, PlayerInteractEvent, BlockBreakEvent, BlockPlaceEvent
from endstone import ColorFormat, GameMode
from endstone.form import ActionForm
from endstone.inventory import ItemStack
import yaml
import csv
import time
import uuid as uuid_lib
from pathlib import Path
from enum import Enum
from typing import Dict, List, Optional, Any

# --- VERSION 0.10.x SPECIFIC IMPORTS ---
try:
    # In 0.10.18, Potion effects are usually here
    from endstone.actor import PotionEffect, PotionEffectType
except ImportError:
    PotionEffect = None
    PotionEffectType = None

class GameState(Enum):
    LOBBY = "LOBBY"
    MATCH = "MATCH"
    ROLLBACK = "ROLLBACK"

class RepeatingTask:
    """
    Handles repeating logic for 0.10.18.
    Uses 'schedule_delayed_task' instead of 'run_task_later'.
    """
    def __init__(self, plugin, task_func, delay: int, period: int):
        self.plugin = plugin
        self.task_func = task_func
        self.period = period
        self.running = True
        self.task = None
        # INITIAL CALL
        self._schedule(delay)

    def _schedule(self, ticks: int):
        if self.running:
            # FIX: In 0.10.18 the method is schedule_delayed_task
            self.task = self.plugin.server.scheduler.schedule_delayed_task(
                self.plugin, self._run, ticks
            )

    def _run(self):
        if not self.running: return
        try:
            self.task_func()
        except Exception as e:
            self.plugin.logger.error(f"Task Error: {e}")
        
        self._schedule(self.period)

    def cancel(self):
        self.running = False

class PlayerData:
    def __init__(self, uuid: str):
        self.uuid = uuid
        self.state = GameState.LOBBY
        self.rollback_buffer = []
        self.csv_path = None
        self.current_kit = None

# --- MAIN PLUGIN ---

class FortCore(Plugin):
    api_version = "0.5"

    def __init__(self):
        super().__init__()
        self.player_data: Dict[str, PlayerData] = {}
        self.plugin_config = {}
        self.teleport_cooldown = {}
        self.rollback_tasks = {}
        self.flush_task = None
        self.rollback_dir = None

    def on_enable(self) -> None:
        self.load_config()
        self.register_events(self)
        
        self.rollback_dir = Path(self.data_folder) / "rollbacks"
        self.rollback_dir.mkdir(parents=True, exist_ok=True)
        
        # Start Flush Task (Every 1200 ticks / 60s)
        self.flush_task = RepeatingTask(self, self.flush_all_buffers, 1200, 1200)
        self.logger.info(f"{ColorFormat.GREEN}FortCore v1.4 (Endstone 0.10.18) Enabled!{ColorFormat.RESET}")

    def on_disable(self) -> None:
        if self.flush_task: self.flush_task.cancel()
        for t in self.rollback_tasks.values(): t.cancel()

    def load_config(self):
        path = Path(self.data_folder) / "config.yml"
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            default = {
                "lobby_spawn": {"x": 0.5, "y": 100, "z": 0.5, "world": "world"},
                "maps": [{"name": "Default Arena", "spawn": {"x": 100, "y": 64, "z": 100}, "world": "world"}],
                "kits": [{"name": "Standard", "maxPlayers": 10}]
            }
            with open(path, 'w') as f: yaml.dump(default, f)
            self.plugin_config = default
        else:
            with open(path, 'r') as f: self.plugin_config = yaml.safe_load(f)

    def reset_player(self, player):
        player.game_mode = GameMode.SURVIVAL
        player.inventory.clear()
        
        cfg = self.plugin_config.get("lobby_spawn", {})
        level = self.server.get_level(cfg.get("world", "world"))
        if level:
            player.teleport(level, cfg.get("x"), cfg.get("y"), cfg.get("z"))

        # Slot 9 (Index 8) Menu Item
        player.inventory.set_item(8, ItemStack("minecraft:lodestone_compass", 1))

        if PotionEffect and PotionEffectType:
            try:
                # Weakness (Type 18) - 1 day duration
                player.add_effect(PotionEffect(PotionEffectType.WEAKNESS, 1728000, 255, False))
            except: pass

    @event_handler
    def on_join(self, event: PlayerJoinEvent):
        # schedule_delayed_task for 0.10.18
        self.server.scheduler.schedule_delayed_task(
            self, lambda: self.reset_player(event.player), 5
        )

    @event_handler
    def on_interact(self, event: PlayerInteractEvent):
        if event.item and event.item.type == "minecraft:lodestone_compass":
            event.cancelled = True
            self.open_menu(event.player)

    def open_menu(self, player):
        form = ActionForm()
        form.title = "FortCore Select"
        kits = self.plugin_config.get("kits", [])
        for i, k in enumerate(kits):
            name = k.get("name")
            form.button(f"{name}", on_click=lambda p, idx=i: self.join_match(p, idx))
        form.send(player)

    def join_match(self, player, idx):
        pid = str(player.unique_id)
        data = self.player_data.setdefault(pid, PlayerData(pid))
        
        kit = self.plugin_config["kits"][idx]
        map_info = self.plugin_config["maps"][idx]
        
        data.state = GameState.MATCH
        data.current_kit = kit["name"]
        data.csv_path = self.rollback_dir / f"rollback_{pid}.csv"
        
        # Clear CSV
        with open(data.csv_path, 'w', newline='') as f:
            csv.writer(f).writerow(["ts", "act", "x", "y", "z", "block"])

        level = self.server.get_level(map_info["world"])
        s = map_info["spawn"]
        player.teleport(level, s["x"], s["y"], s["z"])
        player.send_message(f"{ColorFormat.GOLD}Entering {map_info['name']}")

    @event_handler(priority=EventPriority.MONITOR)
    def on_break(self, event: BlockBreakEvent):
        if not event.cancelled: self.record(event.player, "break", event.block)

    @event_handler(priority=EventPriority.MONITOR)
    def on_place(self, event: BlockPlaceEvent):
        if not event.cancelled: self.record(event.player, "place", event.block)

    def record(self, player, action, block):
        pid = str(player.unique_id)
        if pid in self.player_data and self.player_data[pid].state == GameState.MATCH:
            self.player_data[pid].rollback_buffer.append([time.time(), action, block.x, block.y, block.z, block.type])

    def flush_all_buffers(self):
        for pid, data in self.player_data.items():
            if data.rollback_buffer and data.csv_path:
                with open(data.csv_path, 'a', newline='') as f:
                    csv.writer(f).writerows(data.rollback_buffer)
                data.rollback_buffer.clear()

    @event_handler
    def on_death(self, event: PlayerDeathEvent):
        pid = str(event.entity.unique_id)
        if pid in self.player_data and self.player_data[pid].state == GameState.MATCH:
            self.start_rollback(pid)

    def start_rollback(self, pid):
        data = self.player_data[pid]
        self.flush_all_buffers()
        data.state = GameState.ROLLBACK
        
        actions = []
        if data.csv_path.exists():
            with open(data.csv_path, 'r') as f:
                reader = list(csv.DictReader(f))
                actions = reader[::-1] # Reverse order for undo

        # Process 2 blocks every 10 ticks (0.5s)
        self.rollback_tasks[pid] = RepeatingTask(self, lambda: self.step_rollback(pid, actions), 10, 10)

    def step_rollback(self, pid, actions):
        for _ in range(2):
            if not actions:
                self.rollback_tasks[pid].cancel()
                self.finish_match(pid)
                return
            a = actions.pop(0)
            try:
                lvl = self.server.get_level("world")
                b = lvl.get_block_at(int(a['x']), int(a['y']), int(a['z']))
                b.type = "minecraft:air" if a['act'] == 'place' else a['block']
            except: pass

    def finish_match(self, pid):
        player = self.server.get_player(uuid_lib.UUID(pid))
        if player: self.reset_player(player)
        data = self.player_data.get(pid)
        if data:
            if data.csv_path.exists(): data.csv_path.unlink()
            data.state = GameState.LOBBY

    def on_command(self, sender, command, args) -> bool:
        if command.name == "out" and hasattr(sender, "unique_id"):
            self.start_rollback(str(sender.unique_id))
            return True
        return False
