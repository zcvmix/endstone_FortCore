# FortCore - High-performance PvP Core Plugin for Endstone
# Version 1.3 - Fixed for Latest Endstone API structure

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

# --- CRITICAL FIX: LATEST API IMPORTS ---
# In the latest versions, PotionEffect lives in the actor or inventory modules
try:
    from endstone.actor import PotionEffect, PotionEffectType
except ImportError:
    try:
        from endstone.inventory import PotionEffect, PotionEffectType
    except ImportError:
        # Fallback for older/development versions
        PotionEffect = None
        PotionEffectType = None

# --- HELPER CLASSES ---

class GameState(Enum):
    LOBBY = "LOBBY"
    QUEUE = "QUEUE"
    TELEPORTING = "TELEPORTING"
    MATCH = "MATCH"
    ROLLBACK = "ROLLBACK"
    END = "END"

class RepeatingTask:
    """Fixes 'Scheduler object has no attribute run_task_timer'"""
    def __init__(self, plugin, task_func, delay: int, period: int):
        self.plugin = plugin
        self.task_func = task_func
        self.period = period
        self.running = True
        self.task = None
        self.plugin.server.scheduler.run_task_later(self.plugin, self._run, delay)

    def _run(self):
        if not self.running: return
        try:
            self.task_func()
        except Exception as e:
            self.plugin.logger.error(f"Repeating task error: {e}")
        if self.running:
            self.task = self.plugin.server.scheduler.run_task_later(self.plugin, self._run, self.period)

    def cancel(self):
        self.running = False

class PlayerData:
    def __init__(self, uuid: str):
        self.uuid = uuid
        self.state = GameState.LOBBY
        self.rollback_buffer = []
        self.csv_path = None
        self.current_kit = None
        self.current_map = None

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

    def on_enable(self) -> None:
        self.load_config()
        self.register_events(self)
        
        self.rollback_dir = Path(self.data_folder) / "rollbacks"
        self.rollback_dir.mkdir(parents=True, exist_ok=True)
        
        # Start Flush Task (Every 60s)
        self.flush_task = RepeatingTask(self, self.flush_all_buffers, 1200, 1200)
        self.logger.info("FortCore v1.3 Enabled successfully.")

    def on_disable(self) -> None:
        if self.flush_task: self.flush_task.cancel()
        for t in self.rollback_tasks.values(): t.cancel()

    def load_config(self):
        path = Path(self.data_folder) / "config.yml"
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            default = {
                "lobby_spawn": {"x": 0.5, "y": 100, "z": 0.5, "world": "world"},
                "maps": [{"name": "Arena 1", "creator": "Staff", "spawn": {"x": 100, "y": 64, "z": 100}, "world": "world"}],
                "kits": [{"name": "Starter", "creator": "Staff", "maxPlayers": 10}]
            }
            with open(path, 'w') as f: yaml.dump(default, f)
            self.plugin_config = default
        else:
            with open(path, 'r') as f: self.plugin_config = yaml.safe_load(f)

    def reset_player(self, player):
        """Bulletproof Reset Function"""
        player.game_mode = GameMode.SURVIVAL
        player.inventory.clear()
        
        # Teleport to lobby
        cfg = self.plugin_config.get("lobby_spawn", {})
        level = self.server.get_level(cfg.get("world", "world"))
        if level:
            player.teleport(level, cfg.get("x"), cfg.get("y"), cfg.get("z"))

        # Menu Item
        compass = ItemStack("minecraft:lodestone_compass", 1)
        player.inventory.set_item(8, compass) # Slot 9

        # Safe Potion Application
        if PotionEffect and PotionEffectType:
            try:
                # Type 18 is Weakness in many Bedrock versions
                effect = PotionEffect(PotionEffectType.WEAKNESS, 1000000, 255, False, False)
                player.add_effect(effect)
            except: pass

    @event_handler
    def on_join(self, event: PlayerJoinEvent):
        self.server.scheduler.run_task_later(self, lambda: self.reset_player(event.player), 5)

    @event_handler
    def on_interact(self, event: PlayerInteractEvent):
        if event.item and event.item.type == "minecraft:lodestone_compass":
            event.cancelled = True
            self.open_menu(event.player)

    def open_menu(self, player):
        form = ActionForm()
        form.title = "FortCore"
        kits = self.plugin_config.get("kits", [])
        for i, k in enumerate(kits):
            name = k.get("name")
            count = sum(1 for p in self.player_data.values() if p.state == GameState.MATCH and p.current_kit == name)
            form.button(f"{name} [{count}/{k.get('maxPlayers')}]", on_click=lambda p, idx=i: self.join_match(p, idx))
        form.send(player)

    def join_match(self, player, idx):
        pid = str(player.unique_id)
        data = self.player_data.setdefault(pid, PlayerData(pid))
        
        kit = self.plugin_config["kits"][idx]
        map_info = self.plugin_config["maps"][idx]
        
        # Global Cooldown Check
        now = time.time()
        if now - self.teleport_cooldown.get(kit["name"], 0) < 5:
            player.send_message(f"{ColorFormat.RED}Queue cooldown active!{ColorFormat.RESET}")
            return

        self.teleport_cooldown[kit["name"]] = now
        data.state = GameState.MATCH
        data.current_kit = kit["name"]
        
        # Init Rollback CSV
        data.csv_path = self.rollback_dir / f"rollback_{pid}.csv"
        with open(data.csv_path, 'w', newline='') as f:
            csv.writer(f).writerow(["timestamp", "action", "x", "y", "z", "block"])

        # Teleport
        level = self.server.get_level(map_info["world"])
        s = map_info["spawn"]
        player.teleport(level, s["x"], s["y"], s["z"])
        player.send_message(f"{ColorFormat.GOLD}Match: {map_info['name']} by {map_info['creator']}")

    @event_handler(priority=EventPriority.MONITOR)
    def on_break(self, event: BlockBreakEvent):
        if not event.cancelled: self.record(event.player, "break", event.block)

    @event_handler(priority=EventPriority.MONITOR)
    def on_place(self, event: BlockPlaceEvent):
        if not event.cancelled: self.record(event.player, "place", event.block)

    def record(self, player, action, block):
        data = self.player_data.get(str(player.unique_id))
        if data and data.state == GameState.MATCH:
            data.rollback_buffer.append([time.time(), action, block.x, block.y, block.z, block.type])

    def flush_all_buffers(self):
        for pid, data in self.player_data.items():
            if data.rollback_buffer and data.csv_path:
                with open(data.csv_path, 'a', newline='') as f:
                    csv.writer(f).writerows(data.rollback_buffer)
                data.rollback_buffer.clear()

    @event_handler
    def on_death(self, event: PlayerDeathEvent):
        pid = str(event.entity.unique_id)
        data = self.player_data.get(pid)
        if data and data.state == GameState.MATCH:
            self.start_rollback(pid)

    def on_command(self, sender, command, args) -> bool:
        if command.name == "out" and hasattr(sender, "unique_id"):
            self.start_rollback(str(sender.unique_id))
            return True
        return False

    def start_rollback(self, pid):
        data = self.player_data.get(pid)
        if not data or data.state == GameState.ROLLBACK: return
        
        self.flush_all_buffers()
        data.state = GameState.ROLLBACK
        
        actions = []
        if data.csv_path.exists():
            with open(data.csv_path, 'r') as f:
                reader = list(csv.DictReader(f))
                actions = reader[::-1] # Reverse

        task = RepeatingTask(self, lambda: self.step_rollback(pid, actions), 10, 10)
        self.rollback_tasks[pid] = task

    def step_rollback(self, pid, actions):
        for _ in range(2):
            if not actions:
                self.rollback_tasks[pid].cancel()
                self.finish_match(pid)
                return
            a = actions.pop(0)
            # Logic to set block back to air or original type
            # Note: For strict performance, we assume 'world' context
            try:
                lvl = self.server.get_level("world")
                b = lvl.get_block_at(int(a['x']), int(a['y']), int(a['z']))
                b.type = "minecraft:air" if a['action'] == 'place' else a['block']
            except: pass

    def finish_match(self, pid):
        player = self.server.get_player(uuid_lib.UUID(pid))
        if player: self.reset_player(player)
        data = self.player_data.get(pid)
        if data:
            if data.csv_path.exists(): data.csv_path.unlink()
            data.state = GameState.LOBBY
