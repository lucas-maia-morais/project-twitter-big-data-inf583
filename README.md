For the methods where we use real Twitter data, we'll ask you to

1. Create a file credentials.txt under src/main/resources

2. Write the following data, one per line and in the order they are listed:

i. your consumer key

ii. your consumer secret

iii. your access token

iv. you access token secret

You may also want to create a folder data in the parent folder (i.e. data should be a "sibling" of src) where you can drop your Twitter samples in the following structure (or your own if it works well):


```
|___src, pom, target, etc...
|___data
    |___French
    |   |___folder1
    |   |   |___file1
    |   |___folder2
    |   |   |___file2    
    |   |...
    |   |___stop_words_for_french.txt
    |___English
    |   |...
    |...
```