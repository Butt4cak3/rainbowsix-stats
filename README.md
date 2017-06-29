# rainbowsix-stats

This command line tool parses the data Ubisoft published for Rainbow Six: Siege and puts it into an SQLite3 database.

## Getting the data
I did not include the data in this repository. In order to use this program, you have to download a data file from the Rainbow Six website.

[Downloads](https://rainbow6.ubisoft.com/siege/en-us/news/152-293696-16/introduction-to-the-data-peek-velvet-shell-statistics)

## Usage
Download the objectives data file, extract it and place it somewhere in your filesystem. Then run _setup.py_

After a few minutes it should exit without any errors and there should be a new file called _something.sqlite_.

```
$ python3 setup.py data/datadump_S5.csv
$ ls data/
datadump_S5.sqlite  datadump_S5.csv
```
