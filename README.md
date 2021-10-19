![](https://camo.githubusercontent.com/b5eb294b863520d2a059fb7718c941dd475dfdd83379bce47c981589944c8095/687474703a2f2f737061726b2e6170616368652e6f72672f646f63732f6c61746573742f696d672f737061726b2d6c6f676f2d68642e706e67)

![](https://img.shields.io/apm/l/vim-mode)

# Apache Spark Scala Word Count and Word Pair Count

You will find a link below to Mark Twain’s collected works which will be the input for our word count program. As with problem one
your programs should have two command line arguments, the first is the input directory and the second is the output directory. In
processing the words you should normalize the words by:


- Removing the endings “'” (single quote), “--”, “-”, “'s”, “ly”, “ed”, “ing”, “ness”, “)“, “_”, “;”,“?”, “!”, “,”, “:”.
- Convert all words to lower case
- Remove leading “‘“ (single quote), “”” (double quote), “(“ and “_”.
  
1. Produce the standard word count but the output needs to be sorted by the number of times the word occurs in decreasing
order. The final output should be in a single file even if we use multiple reducers.

2. The second word program counts the occurrence of unordered word pairs. Each time two words occur next to each other we
count that as a pair. For example in the sentence a cat a rat a bat” we have “a cat” occurs twice, “a rat” occurs twice, “a bat”
occurs once as we do not consider the order of the words. The last word in a line is considered as occurring before the first
word in the next line. A few word pairs will not be counted if the file processed by multiple map instances. As in part a) we
want the output sorted by the count in decreasing order

### Prerequisites 

Install Spark locally

#### Setup Instruction

```
Install spark using homebrew

brew install scala

brew install apache-spark
```

### Suggested IDE
[IntelliJ IDEA](https://www.jetbrains.com/idea/download/#section=mac)

### Download Input file

[Mark Twain collection](https://www.gutenberg.org/files/3200/3200.zip)
