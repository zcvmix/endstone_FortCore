from endstone.plugin import Plugin
from endstone.command import Command, CommandSender
from endstone.event import event_handler, EventPriority, PlayerJoinEvent, PlayerDeathEvent, PlayerQuitEvent, PlayerInteractEvent, BlockBreakEvent, BlockPlaceEvent
from endstone import ColorFormat, GameMode
from endstone.form import ActionForm
from endstone.inventory import ItemStack
from endstone.actor import PotionEffect, PotionEffectType
import yaml
import csv
import time
import uuid as uuid_lib
from pathlib import Path
from enum import Enum
from typing import Dict, List, Optional, Any

# --- GLOBAL SETTINGS ---
GLOBAL_TP_COOLDOWN = 5.0
last_tp_time = 0.0

class GameState(Enum):
    LOBBY = "LOBBY"
    QUEUE = "QUEUE"
    TELEPORTING = "TELEPORTING"
    MATCH = "MATCH"
    ROLLBACK = "ROLLBACK"
    END = "END"

class PlayerData:
    def __init__(self, uuid: str):
        self.uuid = uuid
        self.state = GameState.LOBBY
        self.rollback_buffer = [] # RAM Cache
        self.csv_path = None
        self.current_kit = None
        self.current_map = None

class FortCore(Plugin):
    api_version = "0.5"
    
    # Command registration for tab-completion
    commands = {
        "out": {
            "description": "Leave the match and start rollback.",
            "usages": ["/out"],
            "permissions": ["fortcore.command.out"]
        }
    }

    def __init__(self):
        super().__init__()
        self.player_data: Dict[str, PlayerData] = {}
        self.plugin_config = {}
        self.rollback_tasks = {}
        self.rollback_dir = None

    def on_enable(self) -> None:
        self.load_config()
        self.register_events(self)
        
        self.rollback_dir = Path(self.data_folder) / "rollbacks"
        self.rollback_dir.mkdir(parents=True, exist_ok=True)
        
        # Flush RAM to Disk every 60s (1200 ticks)
        # Unified run_task(plugin, task, delay, period)
        self.server.scheduler.run_task(self, self.flush_all_buffers, delay=1200, period=1200)
        
        self.logger.info(f"{ColorFormat.AQUA}FortCore v1.1.0 Enabled (Endstone 0.10.18 mode)")

    def load_config(self):
        path = Path(self.data_folder) / "config.yml"
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            default = {
                "lobby_spawn": {"x": 0.5, "y": 100, "z": 0.5, "world": "world"},
                "maps": [{"name": "Arena 1", "creator": "Admin", "spawn": {"x": 100, "y": 64, "z": 100}, "world": "world"}],
                "kits": [{"name": "Knight", "creator": "Admin", "maxPlayers": 8}]
            }
            with open(path, 'w') as f: yaml.dump(default, f)
            self.plugin_config = default
        else:
            with open(path, 'r') as f: self.plugin_config = yaml.safe_load(f)

    def reset_player(self, player, teleport=True):
        """Core Reset Logic - GM Survival, Clear All, Potion, Menu Item"""
        player.game_mode = GameMode.SURVIVAL
        
        # Clear Inventory & Effects
        player.inventory.clear()
        # Endstone 0.10.x uses remove_all_effects or similar; adding safety
        try: [player.remove_effect(t) for t in PotionEffectType] 
        except: pass

        # Teleport to Lobby
        if teleport:
            cfg = self.plugin_config.get("lobby_spawn", {})
            level = self.server.get_level(cfg.get("world", "world"))
            if level:
                player.teleport(level, cfg.get("x"), cfg.get("y"), cfg.get("z"))

        # Give Locked Menu Item (Slot 9 = Index 8)
        compass = ItemStack("minecraft:lodestone_compass", 1)
        player.inventory.set_item(8, compass)

        # Apply Weakness 255 Infinite (No Particles)
        # Duration: 1 day in ticks = 1728000
        effect = PotionEffect(PotionEffectType.WEAKNESS, 1728000, 255, False)
        player.add_effect(effect)

    @event_handler
    def on_join(self, event: PlayerJoinEvent):
        pid = str(event.player.unique_id)
        self.player_data[pid] = PlayerData(pid)
        # Delay 5 ticks to ensure world is loaded
        self.server.scheduler.run_task(self, lambda: self.reset_player(event.player), delay=5)

    @event_handler
    def on_interact(self, event: PlayerInteractEvent):
        if event.item and event.item.type == "minecraft:lodestone_compass":
            event.cancelled = True
            self.open_menu(event.player)

    def open_menu(self, player):
        form = ActionForm()
        form.title = "{FortCore}"
        kits = self.plugin_config.get("kits", [])
        
        for i, k in enumerate(kits):
            name = k.get("name")
            current = sum(1 for p in self.player_data.values() if p.state == GameState.MATCH and p.current_kit == name)
            form.button(f"{name} [{current}/{k.get('maxPlayers')}]", on_click=lambda p, idx=i: self.handle_join_request(p, idx))
        form.send(player)

    def handle_join_request(self, player, idx):
        global last_tp_time
        pid = str(player.unique_id)
        data = self.player_data[pid]

        if data.state in [GameState.MATCH, GameState.TELEPORTING]: return

        # Global Cooldown Check
        now = time.time()
        if now - last_tp_time < GLOBAL_TP_COOLDOWN:
            player.send_message(f"{ColorFormat.RED}Global teleport cooldown! Wait {int(GLOBAL_TP_COOLDOWN - (now - last_tp_time))}s.")
            return

        kit = self.plugin_config["kits"][idx]
        map_info = self.plugin_config["maps"][idx]
        
        # Check if full
        current = sum(1 for p in self.player_data.values() if p.state == GameState.MATCH and p.current_kit == kit["name"])
        if current >= kit["maxPlayers"]:
            player.send_message(f"{ColorFormat.RED}Match is full!")
            return

        # Start Join Flow
        last_tp_time = now
        data.state = GameState.TELEPORTING
        data.current_kit = kit["name"]
        data.current_map = map_info["name"]
        data.csv_path = self.rollback_dir / f"rollback_{pid}.csv"
        
        # Clear/Create CSV
        with open(data.csv_path, 'w', newline='') as f:
            csv.writer(f).writerow(["type", "x", "y", "z", "data"])

        # Reset & TP
        self.reset_player(player, teleport=False)
        level = self.server.get_level(map_info["world"])
        s = map_info["spawn"]
        player.teleport(level, s["x"], s["y"], s["z"])
        
        data.state = GameState.MATCH
        player.send_message(f"== {ColorFormat.GOLD}FortCore{ColorFormat.RESET} ==\n"
                           f"{map_info['name']} — By: {map_info['creator']}\n"
                           f"{kit['name']} — By: {kit['creator']}")

    @event_handler(priority=EventPriority.MONITOR)
    def on_break(self, event: BlockBreakEvent):
        if event.cancelled: return
        self.record_action(event.player, "break", event.block.x, event.block.y, event.block.z, event.block.type)

    @event_handler(priority=EventPriority.MONITOR)
    def on_place(self, event: BlockPlaceEvent):
        if event.cancelled: return
        # For place, we record what was replaced (usually air) to revert it
        self.record_action(event.player, "place", event.block.x, event.block.y, event.block.z, event.block_replaced.type)

    def record_action(self, player, act, x, y, z, block_data):
        pid = str(player.unique_id)
        if pid in self.player_data and self.player_data[pid].state == GameState.MATCH:
            self.player_data[pid].rollback_buffer.append([act, x, y, z, block_data])

    def flush_all_buffers(self):
        for data in self.player_data.values():
            if data.rollback_buffer and data.csv_path:
                with open(data.csv_path, 'a', newline='') as f:
                    csv.writer(f).writerows(data.rollback_buffer)
                data.rollback_buffer.clear()

    @event_handler
    def on_death(self, event: PlayerDeathEvent):
        pid = str(event.entity.unique_id)
        data = self.player_data.get(pid)
        
        # Summon Thunderstorm (Lightning Bolt)
        if data and data.state == GameState.MATCH:
            lvl = event.entity.level
            lvl.spawn_entity("minecraft:lightning_bolt", event.entity.location)
        
        # Reset Inventory & Start Rollback
        self.server.scheduler.run_task(self, lambda: self.reset_player(event.entity), delay=1)
        if data and data.state == GameState.MATCH:
            self.start_rollback(pid)

    def on_command(self, sender, command, args) -> bool:
        if command.name == "out" and hasattr(sender, "unique_id"):
            pid = str(sender.unique_id)
            if self.player_data.get(pid, {}).state == GameState.MATCH:
                self.start_rollback(pid)
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
                actions = reader[::-1] # Reverse order for undoing

        # Staggered task: 2 actions every 10 ticks (0.5s)
        task = self.server.scheduler.run_task(self, lambda: self.step_rollback(pid, actions), delay=10, period=10)
        self.rollback_tasks[pid] = task

    def step_rollback(self, pid, actions):
        for _ in range(2): # Process 2 actions
            if not actions:
                # Cleanup
                if pid in self.rollback_tasks:
                    self.server.scheduler.cancel_task(self.rollback_tasks[pid].task_id)
                self.finish_cleanup(pid)
                return
            
            a = actions.pop(0)
            try:
                # Revert block
                lvl = self.server.get_level("world") # Default world context
                b = lvl.get_block_at(int(a['x']), int(a['y']), int(a['z']))
                b.type = a['data'] # Set back to the recorded original state
            except: pass

    def finish_cleanup(self, pid):
        data = self.player_data.get(pid)
        if data:
            if data.csv_path and data.csv_path.exists(): data.csv_path.unlink()
            data.state = GameState.LOBBY
            data.current_kit = None
            data.rollback_buffer.clear()
        
        player = self.server.get_player(uuid_lib.UUID(pid))
        if player: self.reset_player(player)

    @event_handler
    def on_quit(self, event: PlayerQuitEvent):
        pid = str(event.player.unique_id)
        if pid in self.player_data and self.player_data[pid].state == GameState.MATCH:
            self.start_rollback(pid)
