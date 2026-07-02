import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# the EL scripts live in src/ (not a package), so put them on the path
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src" / "prep"))
