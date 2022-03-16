# The columns in the dataset

The dataset contains of ~ 3M rows and 24 columns. The columns are:
1) invoiceanditemnumber: concatenated invoice and line number associated with the liquor order. This provides a unique identifier for the individual liquor products included in the store order.
2) date: date of order.
3) storenumber: unique number assigned to the store who ordered the liquor.
4) storename: name of store who ordered the liquor.
5) address: address of store who ordered the liquor.
6) city: city where the store who ordered the liquor is located.
7) zipcode: zip code where the store who ordered the liquor is located.
8) storelocation: location of store who ordered the liquor. The address, city, state and zip code are geocoded to provide geographic coordinates.
9) countynumber: iowa county number for the county where store who ordered the liquor is located.
10) county: county where the store who ordered the liquor is located.
11) category: category code associated with the liquor ordered.
12) categoryname: category of the liquor ordered.
13) vendornumber: the vendor number of the company for the brand of liquor ordered.
14) vendorname: the vendor name of the company for the brand of liquor ordered.
15) itemnumber: item number for the individual liquor product ordered.
16) itemdescription: description of the individual liquor product ordered.
17) pack: the number of bottles in a case for the liquor ordered.
18) bottlevolumeml: volume of each liquor bottle ordered in milliliters.
19) statebottlecost: the amount that alcoholic beverages division paid for each bottle of liquor ordered.
20) statebottleretail: the amount the store paid for each bottle of liquor ordered.
21) bottlessold: the number of bottles of liquor ordered by the store.
22) saledollars: total cost of liquor order (number of bottles multiplied by the state bottle retail).
23) volumesoldliters: total volume of liquor ordered in liters. (i.e. (bottle volume (ml) x bottles sold)/1,000).
24) volumesold_gallons: total volume of liquor ordered in gallons. (i.e. (bottle volume (ml) x bottles sold)/3785.411784).