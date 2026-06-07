import json
import os
from pathlib import Path
from radar_server.config import PRODUCTS

def export_products() -> None:
    export_data = []
    for p in PRODUCTS:
        export_data.append({
            "id": p.id,
            "label": p.label,
            "bounds": {
                "west": p.geo_bounds.west,
                "south": p.geo_bounds.south,
                "east": p.geo_bounds.east,
                "north": p.geo_bounds.north,
            }
        })
    
    output_path = Path("products.json")
    with output_path.open("w") as f:
        json.dump(export_data, f, indent=2)
        f.write("\n")

if __name__ == "__main__":
    export_products()
