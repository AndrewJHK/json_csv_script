To use the advantech converter, put it in the same directory as the advantech .csv and inside the script, on top, change the name of the csv that you want to convert. </br>
Using json-csv converter put the json files in an input folder - default is the the same folder as the main.py and they will all be processed into the .csv files. </br>
Next all the converted files will land in an output folder, if it does not exist, script will make one.</br>
Now all the .csv files will be plotted one by one, and saved in 'plots' directory. Channels to plot are specified at the bottom of the main.py file.</br>
You can put your own .csv into the output folder to plot it. </br>
However the files should include in the name 'adv' or 'lpb' so the script will recognize which values to look for.
