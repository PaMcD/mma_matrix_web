import json
import os
from datetime import datetime

output_folder = "assets/assets/"

if __name__ == "__main__":

    # create a simple json output file that states the last time this was ran
    os.makedirs(output_folder, exist_ok=True)
    with open(os.path.join(output_folder, "last_run.json"), "w") as f:
        json.dump({"last_run": datetime.now().isoformat()}, f)

    print("This is the main module for data sourcing.")
