# Objective
Given a variation of price in percentage and/or in absolute value, a user gets the list of the products with at least 
one of the two metrics higher or equal to the given variation and the value of the variation.
The variation is calculated between 2 dates. These 2 dates are either:
- the first date and last date of the appearance of the product. 
- the last and before the last date of the appearance of the product.  

To be very precise, there is an ambiguity in the way the question was asked. I made the assumption of having these both 
modes, and they are not selected by the user. However, for purpose of lineage, in the output, I will specify how the 
dates are selected (from_start_delta or latest_delta).

# Assumption 
percentage is rounded to 2 for processing


# How to run it: 
TODO: to be filled


# I/O
## Formal input 
The user provides:  
{  
'absolute_variation': absolute_variation_value,  
'relative_variation': relative_variation_value  
  }  
- absolute means that the requested variation will be expressed as a difference (V1-V0)
- relative means that the requested variation will be expressed as percentage of evolution ((V1-V0)/V0)*100  
- {prefix}_variation_value => float   
  - {prefix}_variation_value means minimum (or equal) value of the desired variation to filter the selected products
  where prefix is absolute or relative

## Formal output 
{  
'product_id': {'variation_type': 'absolute_variation', 'value': absolute_variation}    
...  
  }
variation_type => enum of ['absolute_variation', 'relative_variation'], one of the two values that matched the criteria (there is 
an ambiguity to chose between the two values)

# Examples
## Json example
### These are valid jsons of input for the service:     
{  
'absolute_variation': 5,  
'relative_variation': 2.5  
  }
---
{  
'absolute_variation': 5,  
  }
---
{  
'relative_variation': -1.2,  
  }
---
{  
'absolute_variation': null,  
'relative_variation': 2.5  
  }


### These are invalid jsons:     
{   
'dummy_key': 54  
  }  
because the key is not absolute_variation nor relative_variation
---
{  
'absolute_variation': null,  
'relative_variation': null  
  }  
because at least one of the keys should be not null
---
{  
'absolute_variation': 51,  
'relative_variation': 'dummy_string'  
  }  
because one the keys is not in the right type (here, it would be interesting to discuss with consumers of the service
to set rules ex: ignore if error for one of the keys)
---
{  
'absolute_variation': '51$',  
'relative_variation': '120%'  
  }  
because the keys are not in the right type (here, it would be interesting to discuss with consumers of the service 
to set rules of parsing)

## Functional example
Assuming this is the data   
activity_date product price  
01/01/2021 1 10.00  
01/02/2021 1 20.00  
01/03/2021 1 25.00  

where 1 is the product id. 

For an input of this type:
{
{'absolute_variation': 5,
'relative_variation': 25   
}

The output will be   
{1: {'variation_type': 'absolute_variation', 'value': 5}}    
or    
{1: {'variation_type': 'relative_variation', 'value': 25}}


# Remarks
The data in this context is clean.   
prices are always ints (No units)  
dates are all in the same format dd/mm/yyyy  
ids are always ints  

# Improvements 
In order to add new data, we can process new file to keep only 3 rows per product id:   
start date, before last date, end date  
there for we can work by ingesting data progressively without having to recompute everything  
Moreover it will be a simple process, here is a pseudocode:   
- For every product:
    - if present in new file: 
        delete before last value and add the latest value   
    
The code of the solution relies on this, so it can be easily added later.