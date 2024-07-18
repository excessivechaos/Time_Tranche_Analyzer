import PySimpleGUI as sg
import pandas as pd
import ctypes
import os

# make app dpi aware
try:
    ctypes.windll.shcore.SetProcessDpiAwareness(1)
except Exception:
    pass

__version__ = "v.1.0.0"
__program_name__ = "CSV Merger"

if sg.running_windows():
    font = ("Segoe UI", 12)
else:
    font = ("Arial", 14)

sg.theme("Reddit")
sg.SetOptions(font=font, element_padding=(5, 5))
screen_size = sg.Window.get_screen_size()

def get_next_filename(path: str, base: str, ext: str) -> str:
    """
    Takes a path, base name, and extension.
    Checks if a filename already exists with that filename
    Adds (x) to the filename and returns the complete filename path
    """
    # Create filename
    filename = os.path.join(path, f"{base}{ext}")
    counter = 1
    while os.path.exists(filename):
        filename = os.path.join(path, f"{base}({counter}){ext}")
        counter += 1
    return filename


def merge_csvs(files_list):
    output_df = pd.DataFrame()
    loaded_dfs = {}
    for file in files_list:
        loaded_dfs[file] = pd.read_csv(file)

    # merge into 1 df
    for df in loaded_dfs.values():
        # make sure the columns all match
        for other_df in loaded_dfs.values():
            if df.columns.to_list() != other_df.columns.to_list():
                return "The structure of these CSV files are different, cannot merge"
        output_df = pd.concat([output_df, df], ignore_index=True)

    path = os.path.dirname(files_list[0])
    basename = os.path.splitext(os.path.basename(files_list[0]))[0]
    output_filename = get_next_filename(path, f"{basename}_merged", ".csv")
    output_df.to_csv(output_filename, index=False)
    return True


def main():
    layout = [
        [sg.Text("Select CSV files to merge:")],
        [
            sg.Input(
                key="-FILE-",
                expand_x=True,
            ),
            sg.Button("Browse"),
        ],
        [
            sg.Button("Merge"),
            sg.Push(),
            sg.Text(__version__),
        ]
    ]

    window_size = (int(screen_size[0] * 0.4), int(screen_size[1] * 0.15))
    window = sg.Window(
        "CSV Merger", layout, size=window_size, resizable=True, finalize=True
    )
    error = False
    while True:
        event, values = window.read(timeout=100)
        if event == sg.WIN_CLOSED:
            break

        elif event == "Browse":
            files = sg.popup_get_file(
                "",
                file_types=(("CSV Files", "*.csv"),),
                multiple_files=True,
                no_window=True,
                files_delimiter=";",
            )
            if files:
                if isinstance(files, tuple) and len(files) > 1:
                    file_str = ";".join(files)
                    window["-FILE-"].update(file_str)
                else:
                    sg.popup_no_border(
                        "Please select more than 1 CSV file to merge"
                    )

        elif event == "Merge":
            files_list = values["-FILE-"].split(";")
            for file in files_list:
                file_ext = os.path.splitext(file)[1].lower()
                if file_ext != ".csv":
                    sg.popup_no_border(
                        "One or more of the selected files\ndo not appear to be a csv file!"
                    )
                    error = True
                    break
            if error:
                error = False  # reset
                continue

            result = merge_csvs(files_list)
            if type(result) == str:
                sg.popup_no_border(result)
            else:
                sg.popup_no_border("Success!")

            
    
    window.close()

if __name__ == "__main__":
    main()