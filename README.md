[![codecov](https://codecov.io/gh/hextechpal/segmenter/branch/master/graph/badge.svg?token=5SJ2ZKHIFJ)](https://codecov.io/gh/hextechpal/segmenter)

# segmenter

This library implements kafka like partitions(segments) over redis streams. You can define the number of partitions for a stream and it makes sure that
the messages belonging to single segment is processed in order on a single consumer

### Usage
Check out the tests package to see how it works. 

### Things to do

- [ ] Add more tests 
- [ ] Performance testing
- [ ] Documentation godoc + architecture document
