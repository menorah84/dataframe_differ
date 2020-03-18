# DataFrame DIFFer
#### Use: To get symmetric difference between two given DataFrames and a common key
#### Result: If there is a difference, the output is a JSON dictionary showing:
* in A but not in B 
* in B but not in A
* both in A and in B (with same keys but different values in one or more non-key columns)
