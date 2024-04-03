# Readv2/CTV3 terminology parser

Creates, compresses and makes available online useful files derived from the Readv2 and CTV3 clinical code dictionaries.

For each terminology this creates:

| File           | Description                                                                                       |
| -------------- | ------------------------------------------------------------------------------------------------- |
| defs.json      | For each clinical code, an array of the synonym definitions                                       |
| relations.json | A hierarchy mapping a parent code to an array of its direct children                              |
| trie.json      | A trie of all the words in the terminology - useful for fast word lookups e.g. in an autocomplete |
| words.json     | A map from every word longer than 2 characters to an array of clinical codes containing that word |
