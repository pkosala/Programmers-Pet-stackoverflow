## Programmer's Pet
A platform to find duplicate stack overflow posts.

### Motivation
Stack Overflow is one of the premium information exchange sites for programmers to find answers to coding questions. However, the site is often plagued with **duplicate questions** that generate a messy web of answers for users on the site - creating mild (oft unnecessary) confusion. This increases the confusion among users as to which post to follow for the right solution.

Despite this problem stack overflow relies on a handful of users to find duplicates in the system.
In order for a new user to know an existing post can answer his question, he should be able to enter his complete description and then look for similar posts. But stackoverflow currently restricts the search query to 240 characters which clearly is not sufficient for **finding duplicates while user describes his query**.

Programmer's pet is motivated by following limitations:
	*  Provide a platform for users to find similar posts by entering a complete description
	*  A new way of finding similar posts by searching using **code**

This application can be used in following ways:
	*  Embed it into stack overflow to recommend possible duplicates while user enters his query as a new post
	*  A platform for developers to enter code and search for similar scenarios other developers came across


## Implementation Overview
#### Dataset
	*  Stack Overflow data dump, available as a subset of the [Stack Exchange data dump](https://archive.org/details/stackexchange).
	*  The Stack Overflow dataset is also accessible on [Google Big Query](https://cloud.google.com/bigquery/public-data/stackoverflow).
	*  50M posts from July 2008 till December 2018 and data size is around 50GB.
	*  Each post is tagged by technologies associated with it.
	*  Each post has title, description, answers and comments.

#### Preprocessing
* Stack Overflow posts are first preprocessed into question text bodies.
	*  **question text body**: question title concatenated with the question body
	*  Each question text body is tokenized, stripped of stop words, and stemmed.
	*  *code**: Code is extracted from question body
	*  Each question text body is tokenized, non-ascii characters and extra white spaces are removed.
	*  Tri-gram words are generated of both question text body and code are generated.


#### Generating Hash Vectors - MinHashLSH, Stack Overflow
* Trigram words are hashed using binasccii library provided by python.
* The hashed representation is still variable in length, so we hash it again to a fixed length vectors by randomly projecting them on different hyperplanes.
* The fixed length vector size depending on how many hyperplanes the data is projected on. I have used 10 hyperplanes.
* The fixed size vectors are stored in postgresql along with title, postid, array of tags.
* Same procedure is repeated for code but with additional filter that the code should be a minimum of 300 characters long.

#### Finding duplicates for user query
* User query is converted to hash vectors and compared with hash vectors stored in PostgreSQL database.
* For this use case, we used MinHash Jaccard similarity as the similarity metric.
* However, provided the large body of Natural Language Processing and Machine Learning research in the field of semantic similarity and duplicate document detection, this metric could be replaced by a better similarity measure (such as an Machine Learning model) for better performance. 


### Architecture
![Architecture](https://github.com/pkosala/Programmers-Pet-stackoverflow/blob/master/SparkSOF/src/imgs/Pipeline.PNG)
###
## Engineering Challenges and Conclusions


### Out of memory exceptions while batch processing
	*  Broadcast hash coefficients
	*  Process batch of 4M records at a time.
Improved processing time by 75 % using a 4 node spark cluster as opposed to a standalone CPU

### Finding similar posts from among 50M posts
	*  Narrowed down the search set by tags
	*  GIST Indexes on tags and hash vectors
This resulted in reduced query processing time by 60%

## Limitations
* LSH is sensitive to slight changes in word, spellings and order
* Applying LSH directly on code works poorly but performs decently on sufficiently similar texts as it does not consider user coding style.
* Even the change of variable names and loops cannot be detected with this sytem

## Future Work
* Use Glove vectors to represent words and then apply minHash
* Use language specific source code tokenizer to replace original code with tokenized code.
* Use better algorithms like winnowing and Rabin-karp to find duplicates. Need to identify if pre-calculated features can be stored similar to hash vectors in case of LSH

### On duplicate questions
* Duplicate questions are **very sparse in the dataset**, as shown by the low number of questions detected in the sizable subset. This suggests that there may be further avenues to reduce pairwise corpus comparisons for this use case. 
* While pure question deduplication is often a Machine Learning problem where models are trained to detect semantic similarity, MinHashLSH exact similarity (Jaccard similarity) showed **decent accuracy** in identifying near-duplicate questions on questions and their question bodies. 

## References
[1] [Stanford CS246 Lecture Slides on MinHashLSH (2015)](http://snap.stanford.edu/class/cs246-2015/slides/03-lsh.pdf)

[2] [Mining of Massive Datasets, Chapter 3 (2010)](http://infolab.stanford.edu/~ullman/mmds/ch3a.pdf)

[3] [Stanford CS345 Lecture Slides on LSH (2006)](http://infolab.stanford.edu/~ullman/mining/2006/lectureslides/cs345-lsh.pdf)

