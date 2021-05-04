# json2htmls

**json2htmls** is a package that converts json (dict) to html tables.


## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install json2htmls.

```bash
pip install json2htmls
```

## Available methods 
```python
json_to_html - converts json (dict) to html
normalize - normalizes json (dict) , converts values to strings
builder - main method that generates tables
json_arrays_to_dictionary - converts array of objects to objects , with suffixes
json_arrays_destroy - removes arrays from source
```

## Usage
Example

```python
from json2htmls import json_to_html
import json
with open('tests/sample.json') as json_file: # warning currently keys with values are not supported , e.g. "key": ["1", "2"]
    data = json.load(json_file)
source , html = json_to_html(data) # source is the dictionary , html is the html itself
```

Result , use [htmlviewer](https://codebeautify.org/htmlviewer) to see the result
```html
<!DOCTYPE html>
<html>
  <head>
    <title>Report</title>
    <style>table { page-break-inside:auto } tr { page-break-inside:avoid; page-break-after:auto } thead { display:table-header-group } tfoot { display:table-footer-group } .pointer { display: block; clear: both; page-break-after: always; } .clearfix { overflow: auto; clear: both } .clearfix::after { content: &quot;&quot;; clear: both; display: table; } body { margin: 0 7rem; } table { width: 100%; font-size: 0.8rem; } th,td { border: 0.11rem solid black; } table, th,td { border-collapse: collapse; padding: 0.25rem 1rem; text-align: left; margin: 0; } tr:nth-child(even) { background-color: #cccccc61; } </style>
  </head>
  <body>
    <table class="clearfix" id="table_id">
      <tbody>
        <div class="pointer"></div>
        <h3>General</h3>
        <tr>
          <th>Id</th>
          <td>0001</td>
        </tr>
        <tr>
          <th>Type</th>
          <td>donut</td>
        </tr>
        <tr>
          <th>Name</th>
          <td>Cake</td>
        </tr>
        <tr>
          <th>Ppu</th>
          <td>0.55</td>
        </tr>
        <tr></tr>
      </tbody>
    </table>
    <table class="clearfix" id="table_batter" style="page-break-inside: avoid; margin-top: 2rem;">
      <tbody>
        <tr id="row_heading_batter_heading">
          <td colspan="2">
            <h3 class="clearfix">Batter - 4</h3>
          </td>
        </tr>
        <tr id="row_heading_batter_0">
          <th>Id</th>
          <th>Type</th>
        </tr>
        <tr id="row_batter_0">
          <td>1001</td>
          <td>Regular</td>
        </tr>
        <tr id="row_batter_1">
          <td>1002</td>
          <td>Chocolate</td>
        </tr>
        <tr id="row_batter_2">
          <td>1003</td>
          <td>Blueberry</td>
        </tr>
        <tr id="row_batter_3">
          <td>1004</td>
          <td>Devil's Food</td>
        </tr>
      </tbody>
    </table>
    <h3>Batters</h3>
    <table class="clearfix" id="table_batters">
      <tbody></tbody>
    </table>
    <table class="clearfix" id="table_topping" style="page-break-inside: avoid; margin-top: 2rem;">
      <tbody>
        <tr id="row_heading_topping_heading">
          <td colspan="2">
            <h3 class="clearfix">Topping - 7</h3>
          </td>
        </tr>
        <tr id="row_heading_topping_0">
          <th>Id</th>
          <th>Type</th>
        </tr>
        <tr id="row_topping_0">
          <td>5001</td>
          <td>None</td>
        </tr>
        <tr id="row_topping_1">
          <td>5002</td>
          <td>Glazed</td>
        </tr>
        <tr id="row_topping_2">
          <td>5005</td>
          <td>Sugar</td>
        </tr>
        <tr id="row_topping_3">
          <td>5007</td>
          <td>Powdered Sugar</td>
        </tr>
        <tr id="row_topping_4">
          <td>5006</td>
          <td>Chocolate with Sprinkles</td>
        </tr>
        <tr id="row_topping_5">
          <td>5003</td>
          <td>Chocolate</td>
        </tr>
        <tr id="row_topping_6">
          <td>5004</td>
          <td>Maple</td>
        </tr>
      </tbody>
    </table>
  </body>
</html>
```

## Contributing

Pull requests are welcome. Open issues addressing pull requests.

## License

[MIT](https://choosealicense.com/licenses/mit/)