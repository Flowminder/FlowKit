The main purpose for the tests in this directory is to pin existing functionality during refactoring. They
run various queries and compare the generated SQL string as well as the resulting dataframe against previous
results. There is probably no harm in keeping them beyond the refactorings for which they are intended, but
we may also consider integrating them into the rest of the test suite at a later point in time.