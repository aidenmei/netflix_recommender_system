public class Driver {

    public static void main(String[] args) throws Exception {

        if (args == null || args.length < 9) {
            String title = "In Driver.main: main function needs at least 9 arguments:\n";
            String args0 = "args0: dir of raw data input\n";
            String args1 = "args1: dir of <userID  movieID:rating> list output\n";
            String args2 = "args2: dir of co-occurence matrix output\n";
            String args3 = "args3: dir of normalized co-occurence matrix output\n";
            String args4 = "args4: dir of <userID  movieID:rating,...,avgRating> list output\n";
            String args5 = "args4: dir of <movie1,movie2,...,movieN> list output\n";
            String args6 = "args4: dir of full rating matrix output\n";
            String args7 = "args4: dir of matrix cell multiplication output\n";
            String args8 = "args5: dir of matrix cell summation output\n";
            throw new Exception(title + args0 + args1 + args2 + args3 + args4 + args5 + args6 + args7 + args8);
        }

        DataDividerByUser dataDividerByUser = new DataDividerByUser();
        CoOccurenceMatrixBuilder coOccurenceMatrixBuilder = new CoOccurenceMatrixBuilder();
        Normalize normalize = new Normalize();
        AverageRating averageRating = new AverageRating();
        MovieListBuilder movieListBuilder = new MovieListBuilder();
        RatingMatrixBuilder ratingMatrixBuilder = new RatingMatrixBuilder();
        MatrixMultiplication multiplication = new MatrixMultiplication();
        MatrixSummation summation = new MatrixSummation();

        String rawDataInputDir = args[0];
        String userMovieRatingListOutputDir = args[1];
        String coOccurenceMatrixDir = args[2];
        String normalizeDir = args[3];
        String averageRatingDir = args[4];
        String movieListDir = args[5];
        String ratingMatrixDir = args[6];
        String multiplicationDir = args[7];
        String summationDir = args[8];

        String[] path1 = {rawDataInputDir, userMovieRatingListOutputDir};
        String[] path2 = {userMovieRatingListOutputDir, coOccurenceMatrixDir};
        String[] path3 = {coOccurenceMatrixDir, normalizeDir};
        String[] path4 = {rawDataInputDir, averageRatingDir};
        String[] path5 = {rawDataInputDir, movieListDir};
        String[] path6 = {averageRatingDir, movieListDir, ratingMatrixDir};
        String[] path7 = {normalizeDir, ratingMatrixDir, multiplicationDir};
        String[] path8 = {multiplicationDir, summationDir};

        dataDividerByUser.main(path1);
        coOccurenceMatrixBuilder.main(path2);
        normalize.main(path3);
        averageRating.main(path4);
        movieListBuilder.main(path5);
        ratingMatrixBuilder.main(path6);
        multiplication.main(path7);
        summation.main(path8);
    }

}