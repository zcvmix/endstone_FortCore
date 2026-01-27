"""
FortCore - High-performance PvP Core Plugin for Endstone
Optimized for low-end servers and devices
"""

from endstone_fortcore.fortcore import FortCore
from endstone_fortcore.rollback import RollbackManager, GameState, PlayerData, RollbackAction

__all__ = ["FortCore", "RollbackManager", "GameState", "PlayerData", "RollbackAction"]
__version__ = "1.2.0"