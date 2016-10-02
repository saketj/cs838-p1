This job is done in 2 map-reduce phases. First phase involves TokenizerMapper and TokenizerReducer. Second phase is map only which involves SortMap. 
 * TokenizerMapper: creates <key, value> pairs according to input line where key is sorted representation of line and value is line itself. eg line = "hello" then key = "ehllo" and value ="hello"
 * TokenizerReducer: takes input from TokenizerMapper and creates an intermediate output of the form <-#ofwords> <word_1> <word_2> ....<word_#ofWords>
 * SortMap: takes reads the intermediate input and creates key value pairs of the form key = <-#ofwords> and value = <word_1> <word_2> ... <word_#ofWords>
 * There is no reducer for SortMap, output is sorted on the value of key by default which in our case is negative of number of words so output is generated in decreasing order of sizes.
 * As required output doesn't have key therefore we remove key using a custom output format class "ValueOutputFormat" which just outputs the value and skips the key.
