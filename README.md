# SC4023-Project

## Sample Expected Result
```
$ python main.py ResalePricesSingapore.csv U2123642H
Loading data

---------BASIC STORE---------
              Column          Blocks
               month             381
                town             817
           flat_type             871
               block             273
         street_name            1093
        storey_range             654
      floor_area_sqm             218
          flat_model            1199
 lease_commence_date             109
        resale_price             218
               Total            5833

=========WITHOUT SORTING=========

---------COMPRESSED STORE---------
              Column          Blocks
               month             109
                town              55
           flat_type              55
               block             164
         street_name            1093
        storey_range              55
      floor_area_sqm             218
          flat_model              55
 lease_commence_date             109
        resale_price             218
               Total            2131


Running queries for PASIR RIS from months 4 to 5 in 2022

---------FILTER PERMUTATIONS (ZM OFF; IDX ON)---------
         Permutation               Blocks
('month', 'town', 'area')                3|5|7
('month', 'area', 'town')               3|8|10
('town', 'month', 'area')             55|58|60
('town', 'area', 'month')           55|172|175
('area', 'month', 'town')          218|221|223
('area', 'town', 'month')          218|273|276

---------FILTER PERMUTATIONS (ZM ON; IDX ON)---------
         Permutation               Blocks
('month', 'town', 'area')                3|5|7
('month', 'area', 'town')               3|8|10
('town', 'month', 'area')             51|54|56
('town', 'area', 'month')           51|168|171
('area', 'month', 'town')          218|221|223
('area', 'town', 'month')          218|269|272

---------SHARED SCANS---------
11 block reads
Year,Month,town,Category,Value
2022,04,PASIR RIS,Minimum Price,430000.0
2022,04,PASIR RIS,Average Price,607368.53
2022,04,PASIR RIS,Standard Deviation of Price,115751.83
2022,04,PASIR RIS,Minimum Price per Square Meter,4174.76

=========WITH SORTING=========

---------COMPRESSED STORE---------
              Column          Blocks
               month             109
                town              55
           flat_type              55
               block             164
         street_name            1093
        storey_range              55
      floor_area_sqm             218
          flat_model              55
 lease_commence_date             109
        resale_price             218
               Total            2131


Running queries for PASIR RIS from months 4 to 5 in 2022

---------FILTER PERMUTATIONS (ZM OFF; IDX OFF)---------
         Permutation               Blocks
('month', 'town', 'area')          109|111|113
('month', 'area', 'town')          109|114|116
('town', 'month', 'area')           55|160|162
('town', 'area', 'month')           55|181|286
('area', 'month', 'town')          218|327|329
('area', 'town', 'month')          218|273|378

---------FILTER PERMUTATIONS (ZM ON; IDX OFF)---------
         Permutation               Blocks
('month', 'town', 'area')          109|111|113
('month', 'area', 'town')          109|114|116
('town', 'month', 'area')           55|160|162
('town', 'area', 'month')           55|181|286
('area', 'month', 'town')          218|327|329
('area', 'town', 'month')          218|273|378

---------FILTER PERMUTATIONS (ZM OFF; IDX ON)---------
         Permutation               Blocks
('month', 'town', 'area')                3|5|7
('month', 'area', 'town')               3|8|10
('town', 'month', 'area')             55|58|60
('town', 'area', 'month')           55|181|184
('area', 'month', 'town')          218|221|223
('area', 'town', 'month')          218|273|276

---------FILTER PERMUTATIONS (ZM ON; IDX ON)---------
         Permutation               Blocks
('month', 'town', 'area')                3|5|7
('month', 'area', 'town')               3|8|10
('town', 'month', 'area')             55|58|60
('town', 'area', 'month')           55|181|184
('area', 'month', 'town')          218|221|223
('area', 'town', 'month')          218|273|276

---------INDIVIDUAL SCANS---------
9 block reads for min price
9 block reads for avg price
9 block reads for stddev price
11 block reads for min price/sqm
38 total block reads
Year,Month,town,Category,Value
2022,04,PASIR RIS,Minimum Price,430000.0
2022,04,PASIR RIS,Average Price,607368.53
2022,04,PASIR RIS,Standard Deviation of Price,115751.83
2022,04,PASIR RIS,Minimum Price per Square Meter,4174.76

---------SHARED SCANS---------
11 block reads
Year,Month,town,Category,Value
2022,04,PASIR RIS,Minimum Price,430000.0
2022,04,PASIR RIS,Average Price,607368.53
2022,04,PASIR RIS,Standard Deviation of Price,115751.83
2022,04,PASIR RIS,Minimum Price per Square Meter,4174.76

---------VECTOR AT A TIME---------
9 block reads
Year,Month,town,Category,Value
2022,04,PASIR RIS,Minimum Price,430000.0
2022,04,PASIR RIS,Average Price,607368.53
2022,04,PASIR RIS,Standard Deviation of Price,115751.83
2022,04,PASIR RIS,Minimum Price per Square Meter,4174.76
```