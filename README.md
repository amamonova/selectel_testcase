# Selectel test case
This repository contains simple ETL-pipeline for creating html-file with incident card in specific format.

The main script, `get_incident_card.py`:
- merge two files with incident and revenue data
- transform data (datetime formatting, applying regular expressions, calculating some values, 
creating the top by sorting) 
- load data (using jinja2 for rendering html-file with extracted values)

A data folder contains files with data. A templates folder contains `template.html` - html 
markup for the incident card. 

Also, I've added `card_14.html` file with [https://htmlpreview.github.io/?https://github.com/amamonova/selectel_testcase/blob/master/card_14.html](example).

## Getting Started

Clone this repo.

```shell script
git clone https://github.com/amamonova/selectel_testcase
```

### Prerequisites

Download all requirements:

```shell script
pip install -r requirements.txt 
```  

## Running the script

To start classifier:

```shell script
python get_incident_card.py
```

Generated html-file is saved in the directory closed to script.
