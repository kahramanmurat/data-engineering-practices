import os
import json
import pandas as pd
import glob


def flatten_json(y):
    out = {}

    def flatten(x, name=""):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + "_")
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + "_")
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def main():
    data_directory = "/app/data"
    flattened_data = []

    json_files = glob.glob(os.path.join(data_directory, "**", "*.json"), recursive=True)

    for json_file in json_files:
        with open(json_file, "r") as file:
            try:
                data = json.load(file)
                flattened_data.append(flatten_json(data))
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON in file {json_file}: {e}")

    if not flattened_data:
        print("No valid JSON data found in the specified files.")
        return

    df = pd.DataFrame(flattened_data)
    output_csv_file = "flattened_data.csv"

    df.to_csv(output_csv_file, index=False)
    print(f"Flattened data saved to {output_csv_file}")


if __name__ == "__main__":
    main()
