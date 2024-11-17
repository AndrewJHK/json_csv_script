import json
import csv
from datetime import datetime
from pathlib import Path
import os
import argparse
import matplotlib.pyplot as plot
import pandas as pd

lpb_channel_mapping = {
    "adc1.chan0": "TM2",
    "adc1.chan1": "PT2",
    "adc1.chan2": "PT1",
    "adc1.chan3": "TM1",
    "adc2.chan0": "PT5",
    "adc2.chan1": "PT6",
    "adc2.chan2": "PT4",
    "adc2.chan3": "PT3"
}
adv_channel_mapping = {
    "chan0": "TM1",
    "chan1": "TM2",
    "chan2": "TM3",
    "chan3": "TM4",
    "chan4": "TM5",
    "chan5": "TM6",
    "chan6": "TM7",
    "chan7": "TM8",
    "chan8": "TM9",
    "chan9": "TM10",
    "chan10": "TM11",
    "chan11": "TM12",
    "chan12": "TM13",
    "chan13": "TM14",
    "chan14": "TM15",
    "chan15": "TM16"
}
def json_to_csv(json_data, csv_path):
    with (
        open(f"{csv_path}_lpb.csv", mode='w', newline='') as lpb_csv,
        open(f"{csv_path}_adv.csv", mode='w', newline='') as adv_csv
    ):
        lpb_fieldnames = [
            "header.origin", "header.timestamp_epoch", "header.timestamp_human", "header.counter",
            "data.TM1.raw", "data.TM1.scaled", "data.TM2.raw", "data.TM2.scaled",
            "data.PT1.raw", "data.PT1.scaled", "data.PT2.raw", "data.PT2.scaled",
            "data.PT3.raw", "data.PT3.scaled", "data.PT4.raw", "data.PT4.scaled",
            "data.PT5.raw", "data.PT5.scaled", "data.PT6.raw", "data.PT6.scaled",
            "data.cpu_temperature"
        ]
        adv_fieldnames = [
            "header.origin", "header.timestamp_epoch", "header.timestamp_human", "header.counter",
            "data.usb4716.chan0.raw", "data.usb4716.chan0.scaled", "data.usb4716.chan1.raw", "data.usb4716.chan1.scaled",
            "data.usb4716.chan2.raw", "data.usb4716.chan2.scaled", "data.usb4716.chan3.raw", "data.usb4716.chan3.scaled",
            "data.usb4716.chan4.raw", "data.usb4716.chan4.scaled", "data.usb4716.chan5.raw", "data.usb4716.chan5.scaled",
            "data.usb4716.chan6.raw", "data.usb4716.chan6.scaled", "data.usb4716.chan7.raw", "data.usb4716.chan7.scaled",
            "data.usb4716.chan8.raw", "data.usb4716.chan8.scaled", "data.usb4716.chan9.raw", "data.usb4716.chan9.scaled",
            "data.usb4716.chan10.raw", "data.usb4716.chan10.scaled", "data.usb4716.chan11.raw", "data.usb4716.chan11.scaled",
            "data.usb4716.chan12.raw","data.usb4716.chan12.scaled", "data.usb4716.chan13.raw", "data.usb4716.chan13.scaled",
            "data.usb4716.chan14.raw", "data.usb4716.chan14.scaled","data.usb4716.chan15.raw", "data.usb4716.chan15.scaled",
            "data.cpu_temperature"
        ]
        #LPB initialization
        writer_lpb = csv.DictWriter(lpb_csv, fieldnames=lpb_fieldnames)
        writer_lpb.writeheader()
        last_known_values_lpb = {field: 0 for field in lpb_fieldnames}
        lpb_counter = 0
        #ADV initialization
        writer_adv = csv.DictWriter(adv_csv, fieldnames=lpb_fieldnames)
        writer_adv.writeheader()
        last_known_values_adv = {field: 0 for field in lpb_fieldnames}
        adv_counter = 0

        for record in json_data:
            match record["header"].get("origin"):
                case 100:
                    origin = record["header"].get("origin", 0)
                    timestamp_data = record["header"].get("timestamp", {})
                    low = timestamp_data.get("low", 0)
                    high = timestamp_data.get("high", 0)
                    timestamp_epoch_milliseconds = (high * (2 ** 32)) + low
                    seconds = timestamp_epoch_milliseconds // 1000
                    milliseconds = timestamp_epoch_milliseconds % 1000
                    try:
                        base_timestamp = datetime.fromtimestamp(seconds)
                        timestamp_human = f"{base_timestamp.strftime('%Y-%m-%d %H:%M:%S')}.{milliseconds}"

                    except (OSError, ValueError):
                        timestamp_human = "1970-01-01 00:00:00.000"
                    row_data = {
                        "header.origin": origin,
                        "header.timestamp_epoch": timestamp_epoch_milliseconds,
                        "header.timestamp_human": timestamp_human,
                        "header.counter": lpb_counter
                    }
                    cpu_temp_data = record["data"].get("cpu_temperature")
                    if cpu_temp_data:
                        last_known_values_lpb["data.cpu_temperature"] = cpu_temp_data.get("value", 0)
                    row_data["data.cpu_temperature"] = last_known_values_lpb["data.cpu_temperature"]
                    for adc_key, channels in record["data"].items():
                        if adc_key == "cpu_temperature":
                            continue

                        for channel_key, channel_data in channels.items():
                            full_channel_name = f"{adc_key}.{channel_key}"
                            channel_label = lpb_channel_mapping.get(full_channel_name)

                            if channel_label:
                                raw_column = f"data.{channel_label}.raw"
                                scaled_column = f"data.{channel_label}.scaled"

                                if "raw" in channel_data:
                                    last_known_values_lpb[raw_column] = channel_data["raw"]
                                if "scaled" in channel_data:
                                    last_known_values_lpb[scaled_column] = channel_data["scaled"]

                                row_data[raw_column] = last_known_values_lpb[raw_column]
                                row_data[scaled_column] = last_known_values_lpb[scaled_column]

                    for field in lpb_fieldnames:
                        if field not in row_data:
                            row_data[field] = last_known_values_lpb[field]

                    writer_lpb.writerow(row_data)
                    lpb_counter += 1
######################################################################################################################
                case 130:
                    origin = record["header"].get("origin", 0)
                    timestamp_data = record["header"].get("timestamp", {})
                    low = timestamp_data.get("low", 0)
                    high = timestamp_data.get("high", 0)
                    timestamp_epoch_milliseconds = (high * (2 ** 32)) + low
                    seconds = timestamp_epoch_milliseconds // 1000
                    milliseconds = timestamp_epoch_milliseconds % 1000
                    try:
                        base_timestamp = datetime.fromtimestamp(seconds)
                        timestamp_human = f"{base_timestamp.strftime('%Y-%m-%d %H:%M:%S')}.{milliseconds}"

                    except (OSError, ValueError):
                        timestamp_human = "1970-01-01 00:00:00.000"
                    row_data = {
                        "header.origin": origin,
                        "header.timestamp_epoch": timestamp_epoch_milliseconds,
                        "header.timestamp_human": timestamp_human,
                        "header.counter": adv_counter
                    }
                    cpu_temp_data = record["data"].get("cpu_temperature")
                    if cpu_temp_data:
                        last_known_values_adv["data.cpu_temperature"] = cpu_temp_data.get("value", 0)
                    row_data["data.cpu_temperature"] = last_known_values_adv["data.cpu_temperature"]
                    for field_key, channels in record["data"].items():
                        if field_key == "cpu_temperature":
                            continue

                        for channel_key, channel_data in channels.items():
                            full_channel_name = f"{field_key}.{channel_key}"
                            channel_label = adv_channel_mapping.get(full_channel_name)

                            if channel_label:
                                raw_column = f"data.{channel_label}.raw"
                                scaled_column = f"data.{channel_label}.scaled"

                                if "raw" in channel_data:
                                    last_known_values_adv[raw_column] = channel_data["raw"]
                                if "scaled" in channel_data:
                                    last_known_values_adv[scaled_column] = channel_data["scaled"]

                                row_data[raw_column] = last_known_values_adv[raw_column]
                                row_data[scaled_column] = last_known_values_adv[scaled_column]

                    for field in adv_fieldnames:
                        if field not in row_data:
                            row_data[field] = last_known_values_adv[field]

                    writer_adv.writerow(row_data)
                    adv_counter += 1
                case _:
                    print("dupeczka")
                    continue

def plot_from_csv(csv_path, source, plot_channels, plots_folder_path, counter):
    data = pd.read_csv(f"{csv_path}_{source}.csv")
    if not data.empty:
        start_time = data["header.timestamp_epoch"].iloc[0] / 1000
        plot.figure(figsize=(10, 5))
        for channel in plot_channels:
            if f"data.{channel}.scaled" in data.columns:
                time_in_seconds = (data["header.timestamp_epoch"] / 1000) - start_time
                plot.plot(time_in_seconds, data[f"data.{channel}.scaled"], label=f"{channel}_scaled")
        plot.xlabel("Time [ms]")
        plot.ylabel("Scaled Values")
        plot.title("Plot of Scaled Values")
        plot.legend()
        plot.grid(True)
        plot.tight_layout()
        plot_filename = os.path.join(plots_folder_path, f"{source}_output_scaled_plot{counter}.png")
        plot.savefig(plot_filename)
        plot.show()

def main(input_folder=".", output_folder="output"):
    #PLOTING THEESE
    plot_channels = {"lpb": ["TM1", "TM2", "PT1", "PT2", "PT4"],
                     "adv": ["TM1", "TM15"]}
    #PLOTING THEESE
    os.makedirs(output_folder, exist_ok=True)
    plots_folder_path = os.path.join(output_folder, "plots")
    os.makedirs(plots_folder_path, exist_ok=True)
    csv_counter = 1

    for filename in os.listdir(input_folder):
        if filename.endswith(".json"):
            json_path = os.path.join(input_folder, filename)
            csv_filename = f"{Path(filename).stem}"
            csv_output_path = os.path.join(output_folder, csv_filename)

            with open(json_path, 'r', encoding='utf-8') as json_file:
                json_data = json.load(json_file)
            json_to_csv(json_data, csv_output_path)
            [print(source, plot_channels[source]) for source in plot_channels]
            [plot_from_csv(csv_output_path, source, plot_channels[source], plots_folder_path, csv_counter) for source in plot_channels]
            csv_counter += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert JSON files to CSV in specified folders")
    parser.add_argument("--input_folder", type=str, default=".", help="Folder containing JSON files")
    parser.add_argument("--output_folder", type=str, default="output", help="Folder to save CSV files")
    args = parser.parse_args()

    main(input_folder=args.input_folder, output_folder=args.output_folder)