# Netflix Movie Recommender System

Using MapReduce to implement NETFLIX Movie Recommender System based on Item Collaborative Filtering Algorithm.

## Description
This project implements a Movie Recommender System just like NETFLIX does.<br/>Making use of MapReduce to handle multiplication, summation and normalization of very large matrices and optimize computation.<br/>
- <b>Step 1:</b> Build <b>Co-Occurence Matrix</b> of movies from raw dataset which comes from Netflix Prize Dataset (with a little pre-processing) to describe the similarity between any two of all movies. Then normalize the <b>Co-Occurence Matrix</b>.
- <b>Step 2:</b> Build initial <b>Rating Matrix</b> from the same dataset. To each user, we compute the average rating based on movies he/she has already watched and rated. Then we use the user's average rating to rate the movies he/she has not watched yet. Finally we get a full <b>Rating Matrix</b> with all users' ratings on all movies.
- <b>Step 3:</b> Multiply the <b>Co-Occurence Matrix</b> and the <b>Rating Matrix</b> we got from step 1 and step 2 to get the final rating matrix which can precisely predict ratings for each user on movies he/she has not watched yet. 
- <b>Step 4:</b> Based on the result we got from step 3, we recommend movies that users may be interested in.

## Prerequisites
The raw dataset of this project comes from <b>Netflix Prize Dataset</b>. You can find it on [Kaggle](https://www.kaggle.com/netflix-inc/netflix-prize-data/data).<br/>The test dataset in my project is the same dataset with a little pre-processing so that its format becomes `userID,movieID,rating` in each line. You can download the pre-processed dataset from [here](https://s3-us-west-2.amazonaws.com/wengaoye/MovieBigDataSet.tar).

## TODO
Create a web interface to visualize the recommender system.

## Citation
```
  @misc{ye2017netflixrecommendersystem,
    author = {Wengao Ye},
    title = {Netflix Recommender System},
    year = {2017},
    publisher = {GitHub},
    journal = {GitHub repository},
    howpublished = {\url{https://github.com/elleryqueenhomels/netflix_recommender_system}}
  }
```
