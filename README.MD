## Last-Writer-Wins Element Dictionary Implementation

- Store a timestamp for each key-value pair
- Lookup, add, and remove operations
- Allow updating the value of a key
- A function to merge two dictionaries
   
 
Misc properties:
1. GC is not implemented, but it should in realistic scenario where you cound have hundreds of "revisions"
2. For the simplicity this API is not thread-safe per se, but cound be used in a thread-safe manner from the call site. Or could be reimplemented using TrieMap/ConcurrentHashMap to be thread safe on public API.
3. Implemented using mutable Map of key to SortedMap of timestamps + value tuple, without Sets.

## Run Test Suite:
```sbt test```