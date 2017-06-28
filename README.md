# rainbowsix-stats

This command line tool parses the data Ubisoft published for Rainbow Six: Siege and puts it into an SQLite3 database.

## Usage
Download the objectives data file, place it in a directory called _data/_ and rename it to _objectives.csv_. Then run _setup.py_.

After a few minutes it should exit without any errors and there should be a file in _data/_ called _data.sqlite3_.

```
$ python3 setup.py
$ ls data/
data.sqlite3  objectives.csv
```

## What about the other files?
This project is currently work-in-progress.
