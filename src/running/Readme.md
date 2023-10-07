
# Running analysis

### Instructions on how to refresh data. 
For the initial setup, create a garmin folder somewhere locally and put a `data` folder and an empty `imports.txt` and `activities.csv` file there.
Download the data with the garmin export utility to the `data` folder.
```
garmin-backup --backup-dir=/Users/jimmyrasmussen/Dropbox/garmin/data --format=fit jweibel_22@hotmail.com
```
The garmin-backup cli will keep track of what's already downloaded and only download new data.
The garmin export can be downloaded here: https://github.com/petergardfjall/garminexport

Next, extract training data from the downloaded fit files into an activities.csv file
```
python fit_parser.py /Users/jimmyrasmussen/Dropbox/garmin
```
The updated activities file will be located here `/Users/jimmyrasmussen/Dropbox/garmin/activities.csv`

Lastly run one of tehe notebooks to examine the data!

