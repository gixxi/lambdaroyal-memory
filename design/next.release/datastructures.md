# Datastructures

## Indexes and Constraints

An index are is a special kind of constraint. The constraint contract is fullfilled in order to check BEFORE an insert or an update whether the data fullfiles the constraint (e.g. being unique with respect to a certain attribute)

Furthermore the index serves as speed-up for selecting data based on data attributes rather than the key of the key/value pair the data is incooperating in.

### Indexes are functional

Indexes do not directly refer to atomic attributes of data but to a a comparator function that takes two data items into account and can act on arbitrary attributes or relations between them of that very data.
