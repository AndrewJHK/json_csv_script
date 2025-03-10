import json
import csv
from datetime import datetime
from pathlib import Path
import os
import argparse
import matplotlib.pyplot as plot
import dask.dataframe as dd

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
    "usb4716.chan0": "N20_PRES",
    "usb4716.chan1": "CHAMBER_PRES",
    "usb4716.chan2": "N20",
    "usb4716.chan3": "FUEL",
    "usb4716.chan4": "CH4",
    "usb4716.chan5": "CH5",
    "usb4716.chan6": "CH6",
    "usb4716.chan7": "CH7",
    "usb4716.chan8": "CH8",
    "usb4716.chan9": "CH9",
    "usb4716.chan10": "CH10",
    "usb4716.chan11": "CH11",
    "usb4716.chan12": "CH12",
    "usb4716.chan13": "CH13",
    "usb4716.chan14": "CH14",
    "usb4716.chan15": "CH15"
}


def json_to_csv(json_data, csv_path, fill_with_none=True):
    def get_timestamp(record):
        timestamp_data = record["header"].get("timestamp", {})
        low = timestamp_data.get("low", 0)
        high = timestamp_data.get("high", 0)
        return (high * (2 ** 32)) + low

    sorted_data = sorted(json_data, key=get_timestamp)
    lpb_csv_path = None
    adv_csv_path = None
    match fill_with_none:
        case True:
            lpb_csv_path = f"{csv_path}_none_filled_lpb.csv"
            adv_csv_path = f"{csv_path}_none_filled_adv.csv"
        case False:
            lpb_csv_path = f"{csv_path}_interpolated_lpb.csv"
            adv_csv_path = f"{csv_path}_interpolated_adv.csv"

    with (
        open(lpb_csv_path, mode='w', newline='') as lpb_csv,
        open(adv_csv_path, mode='w', newline='') as adv_csv
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
            "data.N20_PRES.scaled", "data.CHAMBER_PRES.scaled",
            "data.N20.scaled", "data.FUEL.scaled",
            "data.CH4.scaled", "data.CH5.scaled",
            "data.CH6.scaled", "data.CH7.scaled",
            "data.CH8.scaled", "data.CH9.scaled",
            "data.CH10.scaled", "data.CH11.scaled",
            "data.CH12.scaled", "data.CH13.scaled",
            "data.CH14.scaled", "data.CH15.scaled",
            "data.cpu_temperature"
        ]
        # LPB initialization
        writer_lpb = csv.DictWriter(lpb_csv, fieldnames=lpb_fieldnames)
        writer_lpb.writeheader()
        last_known_values_lpb = {field: None for field in lpb_fieldnames}
        lpb_counter = 0
        # ADV initialization
        writer_adv = csv.DictWriter(adv_csv, fieldnames=adv_fieldnames)
        writer_adv.writeheader()
        last_known_values_adv = {field: None for field in adv_fieldnames}
        adv_counter = 0

        for record in sorted_data:
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
                            row_data[field] = last_known_values_lpb[field] if not fill_with_none else None

                    writer_lpb.writerow(row_data)
                    lpb_counter += 1
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
                                scaled_column = f"data.{channel_label}.scaled"
                                last_known_values_adv[scaled_column] = channel_data.get("scaled", last_known_values_adv[
                                    scaled_column])
                                row_data[scaled_column] = last_known_values_adv[scaled_column]

                    for field in adv_fieldnames:
                        if field not in row_data:
                            row_data[field] = last_known_values_adv[field] if not fill_with_none else None

                    writer_adv.writerow(row_data)
                    adv_counter += 1
                case _:
                    continue

    for file_path in [lpb_csv_path, adv_csv_path]:
        deletion = False
        try:
            with open(file_path, 'r', encoding='utf-8') as csv_file:
                reader = csv.reader(csv_file)
                rows = list(reader)
                if len(rows) <= 1:
                    deletion = True
            if deletion:
                os.remove(file_path)
        except FileNotFoundError:
            pass


def plot_from_csv(csv_path, source, plot_channels, plots_folder_path, counter):
    data = dd.read_csv(csv_path, assume_missing=True)
    if data.shape[0].compute() > 0:
        start_time = (data["header.timestamp_epoch"] / 1000).min().compute()
        time_in_seconds = ((data["header.timestamp_epoch"] / 1000) - start_time).compute()

        selected_columns = ["header.timestamp_epoch"] + [f"data.{channel}.scaled" for channel in plot_channels]
        filtered_data = data[selected_columns].compute()
        plot.figure(figsize=(10, 5))
        for channel in plot_channels:
            column_name = f"data.{channel}.scaled"
            if column_name in filtered_data.columns:
                plot.plot(
                    time_in_seconds,
                    filtered_data[column_name],
                    label=f"{channel}_scaled"
                )
        plot.xlabel("Time [ms]")
        plot.ylabel("Scaled Values")
        plot.title(f"Plot of Scaled Values ({source})")
        plot.legend()
        plot.grid(True)
        plot.tight_layout()
        plot_filename = os.path.join(plots_folder_path, f"{source}_output_scaled_plot{counter}.png")
        plot.savefig(plot_filename)
        plot.show()


def plot_all_csv_files(input_folder, plots_folder_path, plot_channels):
    csv_files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]
    csv_counter = 1

    for csv_file in csv_files:
        if "lpb" in csv_file:
            source = "lpb"
        elif "adv" in csv_file:
            source = "adv"
        else:
            continue

        csv_path = os.path.join(input_folder, csv_file)
        plot_from_csv(csv_path, source, plot_channels[source], plots_folder_path, csv_counter)
        csv_counter += 1


def main(input_folder=".", output_folder="output", fill_with_none=True):
    # PLOTING THEESE
    plot_channels = {"lpb": ["TM1", "TM2", "PT1", "PT2", "PT4"],
                     "adv": ["CH14"]}
    # PLOTING THEESE
    os.makedirs(output_folder, exist_ok=True)
    plots_folder_path = os.path.join(output_folder, "plots")
    os.makedirs(plots_folder_path, exist_ok=True)

    for filename in os.listdir(input_folder):
        if filename.endswith(".json"):
            json_path = os.path.join(input_folder, filename)
            csv_filename = f"{Path(filename).stem}"
            csv_output_path = os.path.join(output_folder, csv_filename)

            with open(json_path, 'r', encoding='utf-8') as json_file:
                json_data = json.load(json_file)
            json_to_csv(json_data, csv_output_path, fill_with_none)
    plot_all_csv_files(output_folder, plots_folder_path, plot_channels)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert JSON files to CSV in specified folders")
    parser.add_argument("--input_folder", type=str, default=".", help="Folder containing JSON files")
    parser.add_argument("--output_folder", type=str, default="output", help="Folder to save CSV files")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--fill_with_none", dest="fill_with_none", action="store_true",
                       help="Fill missing fields with None")
    group.add_argument("--no_fill_with_none", dest="fill_with_none", action="store_false",
                       help="Fill missing fields with last known values")
    parser.set_defaults(fill_with_none=True)
    args = parser.parse_args()

    main(input_folder=args.input_folder, output_folder=args.output_folder, fill_with_none=args.fill_with_none)
