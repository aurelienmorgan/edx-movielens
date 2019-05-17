





# memory.limit( size = 128000 ) # not needed when working with 'dev_subset', the development data subset

if( !require( caret ) ) { install.packages( "caret" ) } ; library( caret ) # to create folds
if( !require( tidyverse ) ) { install.packages( "tidyverse" ) } ; library( tidyverse )
if( !require( data.table ) ) { install.packages( "data.table" ) } ; library( data.table )
if( !require( scales ) ) { install.packages( "scales" ) } ; library( scales ) # for plot axis formatting (percentages, mostly)
if( !require( lubridate ) ) { install.packages( "lubridate" ) } ; library( lubridate ) # for the 'seconds_to_period' function
if( !require( gridExtra ) ) { install.packages( "gridExtra" ) } ; library( gridExtra ) # to organize plots

if( !require( coop ) ) { install.packages( "coop" ) ; library( coop ) } # for fast pairwise.complete cosine similarity
if( !require( stats ) ) { install.packages( "stats" ) ; library( stats ) } # for primary components decomposition

# Building R for Windows ; @see 'https://cran.r-project.org/bin/windows/Rtools/'
if( !require( devtools ) ) { install.packages( "devtools" ) } ; library( devtools )
if( devtools::find_rtools( debug = TRUE ) ) {
  if( !require( Rcpp ) ) { install.packages( "Rcpp" ) } ; library( Rcpp )
  Sys.setenv("PKG_CXXFLAGS"="-std=c++11") # C++11 ; @see 'http://gallery.rcpp.org/articles/first-steps-with-C++11/'
}




##################################################
##         'amc_pdf_print' function             ##
##################################################
# convenience method to print the pdf report     #
# with (optional) input parameters.              #
# In addition, allows to access                  #
# 'global environment' variables from within     #
# the 'report printing' session (thus avoiding   #
# to train a model each time).                   #
##################################################
amc_pdf_print <- function(
  paramsList = NULL
  , SumatraPDF_fullpath =
    "C:/PROGRA~1/RStudio/bin/sumatra/SumatraPDF.exe "
  , rootFolderPath = getwd()
) {
  t1 <- proc.time()

  outfile <- file.path( rootFolderPath, "Report.pdf" )

  rmarkdown::render(
    input = file.path( rootFolderPath, "/Report.Rmd" )
    , params = paramsList
    , encoding = "UTF-8"
    , output_file = outfile )
  print( ( proc.time() - t1 )[ "elapsed" ] ) # > 40''
  cmd = paste0(
    SumatraPDF_fullpath
    , "\"", outfile, "\"" )
  shell( cmd = cmd, intern = FALSE, wait = FALSE )
}
##################################################




##############################
## SOURCE DATA FORMAT BEGIN ##
##############################
URL <- "http://files.grouplens.org/datasets/movielens/ml-10m.zip"
fil <- basename( URL )
datFil <- paste0( getwd()
                  , .Platform$file.sep, "orig" )
if( !file.exists( fil ) ) download.file( URL, fil )
if (!dir.exists( datFil ) ) unzip( fil, exdir = datFil )
datFil <- paste0( getwd()
                  , .Platform$file.sep, "orig"
                  , .Platform$file.sep, "ml-10M100K" )

ratings <- fread( input = paste0( datFil, .Platform$file.sep, "ratings.dat" )
                  , sep = ":"
                  , drop = c( 2, 4, 6 )
                  , col.names = c( "userId", "movieId"
                                   , "rating", "timestamp" ))
#View( head( ratings ) )

movies <- str_split_fixed(
  readLines( paste0( datFil, .Platform$file.sep, "movies.dat" ) )
  , "\\::"
  , 3 )
colnames( movies ) <- c("movieId", "title", "genres")
movies <- as.data.frame( movies ) %>%
  mutate( movieId = as.numeric( levels( movieId ) )[ movieId ]
          , title = as.character( title )
          , genres = as.character( genres ) )
#str( movies ) ; View( head( movies ) )
movielens <- left_join( ratings, movies, by = "movieId" )

# "Validation" set will be 10% of 'MovieLens (10M)' data
# => 999,999 observations
set.seed( 1 )
test_index <- createDataPartition( y = movielens$rating
                                   , times = 1, p = 0.1, list = FALSE )
edx <- movielens[ -test_index, ]
temp <- movielens[ test_index, ]
# Make sure 'userId' and 'movieId' in "validation" set
# are also in "edx" set :
validation <- temp %>% 
  semi_join( edx, by = "movieId" ) %>%
  semi_join( edx, by = "userId" )

# Add rows removed from "validation" set back into "edx" set
removed <- anti_join( temp, validation, by = c( "movieId", "userId" ) )
edx <- bind_rows( edx, removed ) # rbind( edx, removed )
rm( datFil, fil, URL
    , movielens, movies, ratings, removed, temp, test_index )
##############################
##  SOURCE DATA FORMAT END  ##
##############################


mean( edx$rating ) # 3.512465



############################################
# create indices for our "k" folds         #
# (used in our "k-folds cross-validation") #
############################################
t1 <- proc.time()
set.seed( 1806 )
cv_k <- 10
folds <- createFolds( y = edx$rating
                      , k = cv_k
                      , list = TRUE
                      , returnTrain = FALSE )
#?createFolds # segment the data by fold for "k-fold cross validation"
print( ( proc.time() - t1 )[ "elapsed" ] ) # elapsed 575.35
#str( folds ) ; View( head( folds ) ) # str( edx$movieId ) # str( edx$userId )
############################################




###################################################
# for development purpose, declare 'dev_subset'   #
# a subset of the source dataset                  #
#                                                 #
# (so as to use that subset while developing and, #
# later run the entire code with the whole thing  #
# once ready)                                     #
###################################################
dev_subset <-
  edx %>%
  filter( userId %in% sample( unique( edx$userId )
                              , size = 500, replace = FALSE ) )
# random variable ; between 5k and 10k distinct movies
length( unique( dev_subset$movieId ) )
# random variable ; around 60,000 ratings:
nrow( dev_subset )
# random variable ; around {3.50 - 3.60} stars
mean( dev_subset$rating )
# "Validation" set will be 10% of 'MovieLens (10M)' data
# => 999,999 observations
set.seed( 1 )
dev_validation_index <- createDataPartition( y = dev_subset$rating
                                   , times = 1, p = 0.1, list = FALSE )
temp <- dev_subset[ dev_validation_index, ]
dev_subset <- dev_subset[ -dev_validation_index, ]
# Make sure 'userId' and 'movieId' in "validation" set
# are also in "edx" set :
dev_validation <- temp %>% 
  semi_join( dev_subset, by = "movieId" ) %>%
  semi_join( dev_subset, by = "userId" )
# Add rows removed from "validation" set back into "dev_subset" set
removed <- anti_join( temp, dev_validation, by = c( "movieId", "userId" ) )
dev_subset <- bind_rows( dev_subset, removed ) # rbind( dev_subset, removed )
rm( removed, temp, dev_validation_index )
## corresponding 'test' subsubset index:
# dev test sub_subset approx. 10% of 'dev_subset'
dev_folds <- createFolds( y = dev_subset$rating
                          , k = cv_k
                          , list = TRUE
                          , returnTrain = FALSE )
rm( cv_k )
###################################################




## ########################################
## 'RMSE' function                       ##
###########################################
# a function that computes the "RMSE"     #
# for vectors of "ratings"                #
# and their corresponding "predictions" : #
# REMINDER: we're optimizing our model    #
#           against the RMSE metric.      #
###########################################
RMSE <- function( true_ratings, predicted_ratings ) {
  sqrt( mean( ( true_ratings - predicted_ratings )^2 ) ) }
###########################################




## ##############################################
## 'to_rating' function                        ##
#################################################
# converts continuous values to 'stars rating'  #
# discreet unit movielens ratings ranging       #
# from "0.5" to "5" stars (with step: 0.5 stars)#
#################################################
# ?round_any
# Rounding to the nearest 5
# @see 'http://r.789695.n4.nabble.com/Rounding-to-the-nearest-5-td863189.html#message863190'
to_rating <- function( x ) { max( min( ( .5 * round( x / .5 ) ), 5 ), .5 ) }
#to_rating( 4.75 ) ; to_rating( 4.25 ) ; to_rating( 4.3 ) ; to_rating( 4.1 ) ; to_rating( -.88 ) ;  to_rating( .88 )
data.frame( raw_prediction = seq( -1, 6, by = .01 ), to_rating = mapply( seq( -1, 6, by = .01 ), FUN = to_rating ) ) %>%
  ggplot( aes( x = raw_prediction, y = to_rating ) ) +
  geom_hline( yintercept = 0 ) + geom_vline( xintercept = 0 ) +
  scale_y_continuous( limits = c( 0, 5.1 ), breaks = seq( 0, 5, by = .5 ), expand=c( 0, 0 ) ) +
  scale_x_continuous( limits = c( -1, 6 ), breaks = seq( 0, 6, by = 1 ), expand=c( 0, 0 ) ) +
  coord_fixed() + theme_bw() +
  theme( panel.grid.minor = element_blank(), panel.border = element_blank()
         , axis.line = element_line( colour = "black" ) ) + 
  xlab( "raw_prediction (continuous variable)" ) + geom_point()
#################################################




## #######################################################
## 'duration_string' function                           ##
##########################################################
# convenience method to custom-format duration strings   #
##########################################################
duration_string <- function(
  time_start
  , time_end = proc.time()
) {
  td <- as.POSIXlt( ( time_end - time_start )[ "elapsed" ]
                    , origin = lubridate::origin )
  round( second( td ) ) -> second( td )
  td <- seconds_to_period( td )
  return( tolower( td ) )
}
##########################################################




## #######################################################
## 'get_regularization_optimization' function           ##
##########################################################
# optimization procedure for the hyperparameter "lambda" #
# (regularization penalty term)                          #
##########################################################
# inputs :                                               #
#    - "train_set" - the source data :                   #
#      user/movie ratings in tidy format                 #
#      with the following column names :                 #
#         o "userId", "movieId", "title", "rating"       #
#    - "test_set" data points used                       #
#      to generate predictions (to be compared           #
#      against 'true ratings'                            #
#    - "lambdas" list of different values                #
#      to be considered for "lambda"                     #
#      (primary components count)                        #
#    - "print_comments" do (or not) show                 #
#      progress info on the console                      #
##########################################################
# resultset (list of objects) :                          #
#    - "mu_hat", "movie_avgs" and "user_avgs"            #
#      regularization parameters                         #
#    - "lambda" - the optimum parameter value            #
#    - "lambda_optimization"                             #
#      the "RMSE" versus "k" dataset                     #
##########################################################
get_regularization_optimization <- function(
  train_set
  , test_set = NULL
  , lambdas
  , print_comments = FALSE
) {
  
  t2 <- proc.time()
  optimization_mode <- ( !is.null( test_set ) &
                           "rating" %in% colnames( test_set ) )

  if( !optimization_mode ) lambdas[ 1 ] -> lambdas

  # estimated average user/movie rating
  mu_hat = mean( train_set$rating )
  
  # movie effect & user effect
  # Regularization via optimizing it against the Penalized Least Squares
  # Choosing the "penalty" term lambda
  if( print_comments ) cat( paste0(
    "Regularization - computing \"movie\" & \"user\" effects:\n|"
    , paste( rep( "=", length( lambdas ) ), collapse = "" )
    , "|\n|" ) )
  models_lambdas_rmses <- sapply( lambdas, function( l ){
    b_i_hat_l <- train_set %>%
      group_by( movieId ) %>%
      summarize( b_i_hat =
                   sum( rating - mu_hat ) /
                   ( n() + l )
                 , title = title[ 1 ]
                 )
    b_u_hat_l <- train_set %>%
      left_join( b_i_hat_l, by = "movieId" ) %>%
      group_by( userId ) %>%
      summarize( b_u_hat =
                   sum( rating - b_i_hat - mu_hat ) /
                   ( n() + l ) )
    if( !is.null( test_set ) ) {
      predicted_ratings <- test_set %>%
        left_join( b_i_hat_l, by = "movieId" ) %>%
        left_join( b_u_hat_l, by = "userId" ) %>%
        mutate( pred = mu_hat + b_i_hat + b_u_hat ) %>%
        .$pred
    }
    if( print_comments ) cat( "-" )
    return( list(
      b_i_hat_l = b_i_hat_l
      , b_u_hat_l = b_u_hat_l
      , RMSE_l = switch( optimization_mode + 1
                         , NULL # returns "NULL" if "!optimization_mode"
                         , RMSE( test_set$rating, predicted_ratings ) ) ) )
  } )
  if( print_comments ) cat( "|" )
  models_lambdas_rmses <- data.frame( t( models_lambdas_rmses ) )
  #View( models_lambdas_rmses )
  if( optimization_mode ) {
    optimized_index <- which.min( models_lambdas_rmses$RMSE_l )
    lambda <- lambdas[ optimized_index ]
    movie_avgs <- models_lambdas_rmses$b_i_hat_l[ optimized_index ][[ 1 ]]
    user_avgs <- models_lambdas_rmses$b_u_hat_l[ optimized_index ][[ 1 ]]
    lambda_optimization <-
      cbind( lambda = lambdas
             , RMSE = models_lambdas_rmses %>% select( RMSE_l ) %>% unlist )
    rm( optimized_index )
  } else {
    movie_avgs <- models_lambdas_rmses$b_i_hat_l[ 1 ][[ 1 ]]
    user_avgs <- models_lambdas_rmses$b_u_hat_l[ 1 ][[ 1 ]]
    lambda <- lambdas
    lambda_optimization <- NULL
  }
  setDT( movie_avgs, key = c( "movieId" ) )
  setDT( user_avgs, key = c( "userId" ) )

  if( print_comments ) cat( paste0(
    " done (", duration_string( t2 ), ")\n" ) )
  rm( lambdas, models_lambdas_rmses, t2 )
  gc( reset = FALSE, full = TRUE, verbose = FALSE )

  return( list( mu_hat = mu_hat
                , user_avgs = user_avgs
                , movie_avgs = movie_avgs
                , lambda = lambda
                , lambda_optimization = lambda_optimization
  ) )
}
##########################################################




## #################################################
## 'get_regularization_residuals_matrix' function ##
####################################################
# inputs :                                         #
#    - "train_set" - the source data :             #
#      user/movie ratings in tidy format           #
#      with the following column names :           #
#         o "userId", "movieId", "rating"          #
#    - "mu_hat", "movie_avgs" and "user_avgs" ;    #
#      the Regularization parameters               #
#    - "print_comments" do (or not) show progress  # 
#      info on the console                         #
####################################################
# resultset : a matrix made up of the user/movie   #
# rating residuals (e.g. the error/loss from       #
# the Regularization)                              #
####################################################
get_regularization_residuals_matrix <- function(
  train_set
  , mu_hat
  , b_u_hat
  , b_i_hat
  
  , print_comments = FALSE
) {
  
  # formating into a matrix of ratings
  t2 <- proc.time()
  if( print_comments ) cat( paste0(
    "Turning tidy dataset into matrix of ratings :\n|"
    , paste( rep( "=", 4 ), collapse = "" )
    , "|\n|" ) )
  ratings_matrix <-
    train_set %>%
    select( userId, movieId, rating ) %>%
    spread( movieId, rating ) %>%
    as.matrix()
  if( print_comments ) cat( "-" )
  rownames( ratings_matrix ) <- ratings_matrix[ , 1 ]
  ratings_matrix <- ratings_matrix[ , -1 ]
  #View( x = head( ratings_matrix[ , 1:10 ], 20 ), title = "train_set_matrix" )
  if( print_comments ) cat( "-" )
  
  # transforming into a matrix of residuals
  # remainder from the "Regularization" approximation
  # (eg. modeling error/loss)
  residuals_matrix <- ratings_matrix
  rm( train_set, ratings_matrix )
  gc( reset = FALSE, full = TRUE )
  residuals_matrix <-
    sweep( residuals_matrix - mu_hat
           , 2, b_i_hat )
  if( print_comments ) cat( "-" )
  residuals_matrix <-
    sweep( residuals_matrix
           , 1, b_u_hat )
  rm( mu_hat, b_i_hat, b_u_hat )
  gc( reset = FALSE, full = TRUE )
  if( print_comments ) cat( paste0(
    "-| done (", duration_string( t2 ), ").\n" ) )
  #View( head( residuals_matrix[ , 1:10 ], 20 ) )
  
  return( residuals_matrix )
}
####################################################




## ####################################################
## 'get_knn_predictions' function                    ##
#######################################################
#                 Rcpp implementation                 #
# for each "userId"/movieId" pair of the test domain, #
# returns one rating prediction                       #
# per value of the "ks" vector                        #
#       (Rcpp function called inside                  #
#        the R 'get_knn_optimization' function)       #
#######################################################
# inputs :                                            #
#    - "ks" list of different values                  #
#      to be considered for "k" (neighbors count)     #
#    - "test_sim_matrix" similarity matrix            #
#      for the "test" movies (one per column)         #
#    - "test_ratings_matrix" rating matrix            #
#      for the "test" users, all movies included      #
#      (from which "neighbor" ratings are picked)     #
#    - "test_domain" list of "userId"/movieId" pairs  #
#      (for which a prediction is expected)           #
#    - "print_comments" do (or not) show              #
#      progress info on the console                   #
#######################################################
# resultset (tidy format ; 4 columns) :               #
#    - "userId"                                       #
#    - "movieId"                                      #
#    - "k"                                            #
#    - "prediction"                                   #
#######################################################
{
Rcpp::sourceCpp( code =
'
#include <Rcpp.h>
#include <queue>
#include <string>     // std::string, std::stoi

using namespace Rcpp;
using namespace std;


std::string duration_string( const float duration_seconds ) {
  int hours, minutes;
  minutes = duration_seconds / 60;
  hours = minutes / 60;

  char hours_numstr[ 3 ];
  if( hours > 0 ) sprintf(hours_numstr, "%u", int(hours));
  std::string hours_suffixe = "h ";
  std::string hours_result = hours > 0 ? hours_numstr + hours_suffixe : "";

  char minutes_numstr[ 3 ];
  if( minutes > 0 ) sprintf(minutes_numstr, "%u", int(minutes%60));
  std::string minutes_suffixe = "m ";
  std::string minutes_result =
    (hours > 0) | (minutes > 0) ? hours_result + minutes_numstr + minutes_suffixe : hours_result;

  char seconds_numstr[ 5 ];
  if( (hours > 0) | (minutes > 0) ) {if( duration_seconds > 0 ) sprintf(seconds_numstr, "%u", int(duration_seconds)%60%60);}
  else { if( duration_seconds > 0 ) sprintf(seconds_numstr, "%.2f", duration_seconds); }
  std::string seconds_suffixe = "s";
  std::string seconds_result = (hours > 0) | (minutes > 0) ? minutes_result + seconds_numstr + seconds_suffixe : seconds_numstr + seconds_suffixe;

  return seconds_result;
}

typedef pair<double, int> Elt;
class mycomparison {
  bool reverse;

  public:
    mycomparison( const bool& revparam=false ) { reverse=revparam; }

    bool operator() (const Elt& lhs, const Elt& rhs) const {
      if ( !reverse ) {
        if( lhs.first == rhs.first ) {
        return ( lhs.second < rhs.second );
        } else {
        return ( lhs.first > rhs.first );
        }
      } else {
        if( lhs.first == rhs.first ) {
          return ( lhs.second > rhs.second );
        } else {
          return ( lhs.first < rhs.first );
        }
      }
    }
};


/** *********************************
* (ordered) indices                 *
* of top "n" elements of vector "v" *
* (using the "mycomparison"         *
* custom comparator)                *
************************************/
std::vector<int> top_n_idx(
NumericVector v
, unsigned int n
) {
  // adapted from @see "htpp://gallery.rcpp.org/articles/top-elements-from-vectors-using-priority-queue/"
  // also @see "https://en.cppreference.com/w/cpp/container/priority_queue"
  // also @see "http://www.cplusplus.com/reference/queue/priority_queue/priority_queue/""
  priority_queue< Elt, vector<Elt>, mycomparison > pq;
  vector<int> result;

  for ( int i = 0; i != v.size() ; ++i ) {
    if ( !isnan( v[i] ) ) {
      if( pq.size() < n )
        pq.push( Elt( v[ i ], i ) );
      else {
        Elt elt = Elt( v[i], i );
        if ( pq.top().first < elt.first ) {
          pq.pop(); // removes the top element
          pq.push( elt );
        }
      }
    }
  }

  result.reserve(pq.size());
  while ( !pq.empty() ) {
    result.insert( result.begin(), pq.top().second + 1 );
    pq.pop();
  }
  /** in case there are not enough "non NA" values => */
  for ( int i = 0; ( i != v.size() ) & ( result.size() < n ) ; ++i ) {
    if ( isnan( v[i] ) ) {
      result.push_back( i + 1 );
    }
  }

  return result ;
}


/** *************************************************
#              return in tidy format                #
#       top-k movies similar to a given "movie_ID"  #
*****************************************************
# getMoviesKnns( int k, sim_matrix )                #
#    - k          - top "k" most similar neighbors  #
#    - sim_matrix - simmilarity matrix              #
#                   - "m" columns (one per movie)   #
#                   - "n" rows (one per potential   #
#                     neighbor)                     #
# => (ordered) resultset :                          #
#    - neighbor_ID                                  #
#    - similarity [0-1]                             #
****************************************************/
DataFrame getMovieKnns(
  std::string movie_ID
  , int k
  , NumericMatrix& sim_matrix
) {
  const CharacterVector xcols = colnames( sim_matrix );
  int colidx = -1;

  int i = 0;
  for( ; i < xcols.size() ; ++i ) {
    if( xcols[ i ] == movie_ID ) colidx = i;
  }

  if( colidx > -1 ) {
    NumericMatrix::Column movie_similarities_vector = sim_matrix( _, colidx );
    std::vector<int> rows_idx = top_n_idx( movie_similarities_vector, k );
    IntegerVector rows_idx_vec( rows_idx.begin(), rows_idx.end() );
    CharacterVector neighborsNamesVec = rownames( sim_matrix );
    IntegerVector neighborsNamesIntVec( neighborsNamesVec.size() );
    std::transform(
      neighborsNamesVec.begin(), neighborsNamesVec.end()
      , neighborsNamesIntVec.begin(), std::atoi );

    IntegerVector topNeighborsColumn( k );
    topNeighborsColumn =
    neighborsNamesIntVec[ rows_idx_vec - 1 ]; // index starts at \'0\'
    NumericVector similaritiesColumn =
    sim_matrix( _, colidx );
    NumericVector topSimilaritiesColumn =
    similaritiesColumn[ rows_idx_vec - 1 ]; // index starts at \'0\'

    return DataFrame::create(
      Rcpp::Named( "neighbor_ID" ) = topNeighborsColumn
      , Rcpp::Named( "similarity" ) = topSimilaritiesColumn
      , Rcpp::Named( "stringsAsFactors" ) = false
    );

  } else {
    return DataFrame::create(
      Rcpp::Named( "neighbor_ID" ) = IntegerVector::create()
      , Rcpp::Named( "similarity" ) = NumericVector::create()
    );
  }
}



/** *************************************************
*                 for a given movie,                *
*       all "k" neighbors rating predictions        *
*          ( each weighted by similarity)           *
*****************************************************
* for each "m movie" =>                             *
*   "r" : a matrix of "users" rows, "max_k" columns *
* (for each "k in 1:max_k" neighbors,               *
*  a vector of length "u users")                    *
****************************************************/
NumericMatrix movieUsers_neighborsRatings(
  const DataFrame& xcpp
  , const DataFrame& nncpp // list of nearest neighbors (ID nd similarity)
  , const int k 
  , const IntegerVector movie_users_IDs_idx
) {

  NumericMatrix resultMatrix( movie_users_IDs_idx.size(), k );

  const CharacterVector neighborsColumn = nncpp[ "neighbor_ID" ];
  const NumericVector similaritiesColumn = nncpp[ "similarity" ];

  for( int X = 0 ; X < k ; ++X ) {
//printf( "%u - %s - %f\\n", X, as<std::string>( neighborsColumn[ X ] ).c_str(), similaritiesColumn[ X ] );
    NumericVector ratingsColmun = xcpp[ as<std::string>( neighborsColumn[ X ] ) ];
    NumericVector usersRatingsColmun = ratingsColmun[ movie_users_IDs_idx ];
    resultMatrix( _ , X ) = usersRatingsColmun * similaritiesColumn[ X ];
  }

  return resultMatrix;
}


IntegerVector nonNA_rowCounts( NumericMatrix x ) {
  IntegerVector result( x.nrow() );
  int rowCount = 0;

  for( int i = 0 ; i < x.nrow() ; ++i ) {
    LogicalVector nonNA_row = !is_na( x( i, _ ) );
    rowCount = 0;
    for ( int j = 0 ; j < x.ncol() ; ++j ) {
      if( nonNA_row[ j ] == true ) rowCount++;
    }
    result( i ) = rowCount;
  }

  return result;
}


/** ***************************************************
* for each "userId"/movieId" pair of the test domain, *
* returns one rating prediction                       *
* per value of the "ks" vector                        *
*******************************************************
* inputs :                                            *
*    - "ks" list of different values                  *
*      to be considered for "k" (neighbors count)     *
*    - "test_sim_matrix" similarity matrix            *
*      for the "test" movies                          *
*    - "test_ratings_matrix" rating matrix            *
*      for the "test" users, all movies included      *
*      (from which "neighbor" ratings are picked)     *
*    - "test_domain" list of "userId"/movieId" pairs  *
*      (for which a prediction is expected)           *
*    - "print_comments" do (or not) show              *
*      progress info on the console                   *
*******************************************************
* resultset (tidy format ; 4 columns) :               *
*    - "userId"                                       *
*    - "movieId"                                      *
*    - "k"                                            *
*    - "prediction"                                   *
******************************************************/
// [[Rcpp::export]]
DataFrame get_knn_predictions(
  IntegerVector ks
  , NumericMatrix test_sim_matrix
  , NumericMatrix test_ratings_matrix
  , DataFrame test_domain // to extract users list for each movie (pairs for which a prediction is expected)
  , bool print_comments = false
) {
  clock_t start;
  double duration;
  if( print_comments ) {
  start = clock();
  std::string prog_bar_str =
  std::string( "Generating the knn predictions :\\n" ) +
  "|" + std::string( 33, \'=\' ) + "|\\n|";
  printf( prog_bar_str.c_str() );
  }
  IntegerVector all_test_movies_column = test_domain[ "movieId" ];
  IntegerVector movieIds = sort_unique( all_test_movies_column );
  const int counter_group_size = ceil( movieIds.size() / 33.0 );

  DataFrame test_ratings_matrix_df = DataFrame( test_ratings_matrix );

  IntegerVector all_test_users_column = test_domain[ "userId" ];
  LogicalVector all_test_rows_idx( all_test_movies_column.size() );

  int max_k = max( ks );

  NumericVector m_rowSums_k;
  NumericVector m_nonNA_rowCounts_k;
  NumericVector m_movieReco_k;
  NumericVector m_rowSums_prior_k;
  NumericVector m_nonNA_rowCounts_prior_k;

  IntegerVector result_userId_column =
  rep( NA_INTEGER, ks.length() * test_domain.nrow() );
  IntegerVector result_movieId_column =
  rep( NA_INTEGER, ks.length() * test_domain.nrow() );
  IntegerVector result_k_column =
  rep( NA_INTEGER, ks.length() * test_domain.nrow() );
  NumericVector result_prediction_column =
  rep( NA_REAL, ks.length() * test_domain.nrow() );

  int firstRowNb = 0;


  for( int m = 0 ; m < movieIds.size() ; ++m ) {
    int movie_ID = movieIds[ m ];
//printf( "%lli - %u\\n", m, movie_ID );


    /** ************************************
    * collect the list of users for which  *
    * the test dataset expect a prediction *
    * for this particular movie            *
    * *************************************/
//printf( "%lli\\n", all_test_movies_column.size() ) ;
    for ( int i = 0 ; i < all_test_movies_column.size() ; ++i ) {
      all_test_rows_idx[ i ] =
        ( all_test_movies_column[ i ] == movie_ID );
    }
    IntegerVector movie_users_IDs =
      all_test_users_column[ all_test_rows_idx == true ];
/*
printf(
  "(%u) - movie %u ; %lli users\\n"
  , ( m + 1 ), movie_ID, movie_users_IDs.size() );
*/


    /** ******************************
    * retrieve the nearest neighbors *
    * for this particular movie      *
    * *******************************/
    DataFrame m_nn =
      getMovieKnns( std::to_string( movie_ID )
                    , max_k, test_sim_matrix );

    /** ***************************************
    * isolate the subset we\'re interested in *
    * for this particular movie               *
    * ****************************************/
    CharacterVector usersNamesVec = rownames( test_ratings_matrix );
    IntegerVector usersNamesIntVec( usersNamesVec.size() );
    std::transform(
      usersNamesVec.begin(), usersNamesVec.end()
      , usersNamesIntVec.begin(), std::atoi );
    IntegerVector movie_users_IDs_idx( movie_users_IDs.size() );
    for( int i = 0 ; i < movie_users_IDs.size() ; i++ ) {
      for( int j = 0 ; j < usersNamesIntVec.size() ; j++ ) {
        if( usersNamesIntVec( j ) == movie_users_IDs( i ) ) {
          movie_users_IDs_idx( i ) = j;
          break; // breaks out of the innermost loop
        }
      }
    }


    /** ****************************************
    * retrieve all neighbors ratings (columns) *
    * for the users (rows) for which
    * a prediction is expected
    * for this particular movie
    *******************************************/
    NumericMatrix m_r =
      movieUsers_neighborsRatings(
      test_ratings_matrix_df, m_nn, max_k
      , movie_users_IDs_idx );
//printf( "%u - %u\\n", m_r.nrow(), m_r.ncol() );
/*
if( movie_ID == 356 ) {
  Environment base = Environment::base_namespace();
  Function saveRDS( "saveRDS" );
  saveRDS( m_nn, "m_nn356" );
  saveRDS( movie_users_IDs_idx, "movie_users_IDs_idx356" );
  saveRDS( test_ratings_matrix_df, "test_ratings_matrix_df" );
  saveRDS( m_r, "m_r356" );
}
*/

    int k;
    int prior_k = -1;
    m_rowSums_prior_k = rep( 0, movie_users_IDs.size() );
    m_nonNA_rowCounts_prior_k = rep( 0, movie_users_IDs.size() );
    for ( int X = 0 ; X < ks.length() ; ++ X ) {
      k = ks[ X ] - 1;  // index starts at \'0\'
      m_rowSums_k =
        m_rowSums_prior_k + rowSums( m_r( _, Range( prior_k + 1, k ) ), true );
      m_nonNA_rowCounts_k =
        m_nonNA_rowCounts_prior_k +
        as<NumericVector>( nonNA_rowCounts( m_r( _, Range( prior_k + 1, k ) ) ) );
      m_movieReco_k =
        m_rowSums_k / m_nonNA_rowCounts_k;
//if( movie_ID == 356 ) printf( "\\n%u - %u - %f / %f", ( k + 1 ), movie_users_IDs( 0 ), m_rowSums_k( 0 ), m_nonNA_rowCounts_k( 0 ) );

      IntegerVector updated_rows_idx =
      seq( firstRowNb, firstRowNb + movie_users_IDs.size() - 1 );
      result_userId_column[ updated_rows_idx ] = movie_users_IDs;
      result_movieId_column[ updated_rows_idx ] = movie_ID;
      result_k_column[ updated_rows_idx ] = k + 1;
      result_prediction_column[ updated_rows_idx ] = m_movieReco_k;
      firstRowNb = firstRowNb + movie_users_IDs.size();

      m_rowSums_prior_k = m_rowSums_k;
      m_nonNA_rowCounts_prior_k = m_nonNA_rowCounts_k;
      prior_k = k;
    }

    if( print_comments ) {
      if(
        ( ( ( m + 1 ) % counter_group_size ) == 0 ) &
        ( ( m + 1 ) != movieIds.size() ) ) { printf( "-" ); }
      }
  }

  DataFrame result = DataFrame::create(
    Rcpp::Named( "userId" ) = result_userId_column
    , Rcpp::Named( "movieId" ) = result_movieId_column
    , Rcpp::Named( "k" ) = result_k_column
    , Rcpp::Named( "prediction" ) = result_prediction_column
    , Rcpp::Named( "stringsAsFactors" ) = false
  );

  if( print_comments ) {
    printf( "-|" );
    duration = ( clock() - start ) / (double) CLOCKS_PER_SEC;
    std::cout << " done: " << duration_string( duration ) << \'\\n\';
  }

  return result;
}
' )
}                                                 #
#######################################################




## #######################################################
## 'get_knn_optimization' function                      ##
##########################################################
#    optimization procedure for the hyperparameter "k"   #
##########################################################
# inputs :                                               #
#    - "train_rating_residuals_matrix" similarity matrix #
#      for the "residual ratings"                        #
#      (remainder after "Regularization")                #
#      on the "training" dataset                         #
#    - "ks" list of different values                     #
#      to be considered for "k" (neighbors count)        #
#    - "test_set" data points used                       #
#      to compare predictions against 'true ratings'     #
#    - "mu_hat", "movie_avgs" and "user_avgs"            #
#      regularization parameters                         #
#    - "print_comments" do (or not) show                 #
#      progress info on the console                      #
##########################################################
# resultset (list of objects) :                          #
#    - "similarity_matrix"                             #
#     the movies cosine similarity matrix                #
#    - "k" - the optimum parameter value                 #
#    - "k_optimization" - the "RMSE" versus "k" dataset  #
#    - "predicted_ratings"                               #
#      the optimized predictions in tidy format          #
#      with colnames "userId", "movieId" and "pred"      #
##########################################################
get_knn_optimization <- function(
  train_rating_residuals_matrix
  , train_sim_matrix = NULL
  , ks

  , test_set
  
  , mu_hat , movie_avgs , user_avgs


  , print_comments = FALSE
) {
  t2 <- proc.time()

  optimization_mode <- ( "rating" %in% colnames( test_set ) )
  if( !optimization_mode ) ks[ 1 ] -> ks

  if( is.null( train_sim_matrix ) ) {
    t3 <- proc.time()
    if( print_comments ) cat( "KNN - Computing the movies cosine similarity matrix.." )
    train_sim_matrix <-
      coop::cosine( train_rating_residuals_matrix, use = "pairwise.complete" )
    train_sim_matrix - diag( ncol( train_sim_matrix ) ) ->
      train_sim_matrix
    if( print_comments ) cat( paste0( " done (", duration_string( t3 ), ")\n" ) )
    rm( t3 )
  }
  
  ###############################################
  # we're gonna evaluate the prediction-ratings #
  # for some pairs of                           #
  # the distinct(test_set$userId),              #
  # distinct(test_set$movieId) domain.          #
  ###############################################
  setDT( test_set, key = c( "movieId", "userId" ) )
  test_domain = unique( test_set[ , c( "movieId", "userId" ) ] )
  test_userIds <-
    as.character( unique( test_domain[ , "userId" ] ) %>% .$userId )
  test_movieIds <-
    as.character( unique( test_domain[ , "movieId" ] ) %>% .$movieId )
#saveRDS( test_movieIds, "test_movieIds_VIII" )
  
  test_ratings_matrix <-
    train_rating_residuals_matrix[ test_userIds, ] # (keep all movies here (all potential neighbors))
  test_sim_matrix <- train_sim_matrix[ , test_movieIds ]
#saveRDS( test_sim_matrix, "test_sim_matrix_VIII" )
  
  sort( unique( replace( ks, ks == 1, 2 ) ) ) -> ks
  ks[ ks < ncol( train_rating_residuals_matrix ) ] -> ks
  rm( train_rating_residuals_matrix )
  
  predictions <-
    get_knn_predictions(
      ks = ks
      , test_sim_matrix = test_sim_matrix
      , test_ratings_matrix = test_ratings_matrix
      , test_domain = test_domain
      , print_comments = print_comments
    )
  setDT( predictions, key = c( "userId", "movieId" ) )
  rm( test_ratings_matrix )
  gc( reset = FALSE, full = TRUE, verbose = FALSE )
  
  # what shall we do for users for which the 'X's movie neighbor
  # gives an 'NA' prediction (average divided by '0' ;
  # none of them has been rated either) ?
  # replace 'NA's with 'mu_hat + b_i_hat + b_u_hat' ?
  # actually makes zero difference when the value of k is high enough
  # (all user/movie pairs has at least one neighbor)
  
  if( optimization_mode ) {
    columns <-
      c( "userId", "movieId", "k", "prediction", "rating" )
  } else {
    columns <-
      c( "userId", "movieId", "k", "prediction" )
  }
  result <-
    merge( merge(
      merge( predictions, test_set
             , by = c( "userId", "movieId" )
             , all = TRUE # full join (for speed)
      )[ , columns, with = FALSE ]
      , movie_avgs, by = c( "movieId" ) )
      , user_avgs, by = c( "userId" ) ) %>%
    mutate( pred =
              case_when(
                !is.finite( prediction ) ~ 0 + mu_hat + b_i_hat + b_u_hat
                , TRUE ~ prediction + mu_hat + b_i_hat + b_u_hat ) )
  setDT( result, key = "k" )
#saveRDS( predictions, "predictions_VIII" )
#saveRDS( result, "result_VIII" )
  rm( predictions )

  if( optimization_mode ) {
    ## actually calculate models RMSEs ##
    show_progress_opt <- getOption( "dplyr.show_progress" )
    options( dplyr.show_progress = FALSE )
    model_rmses <- result %>% group_by( k ) %>%
      do( ( function(x)
        data.frame( RMSE =
                      RMSE( x$rating, x$pred ) ) )( . ) )
    options( dplyr.show_progress = show_progress_opt )
#saveRDS( model_rmses, "model_rmses_VIII" )

    if( print_comments ) cat( paste(
      "Movies neighbors optimization total duration :"
      , duration_string( t2 ), "\n" ) )
    rm( t2 )

    best_k <- model_rmses$k[ which.min( model_rmses$RMSE ) ]
  } else {
    best_k <- ks
    model_rmses <- NULL
  }

  return( list( similarity_matrix = train_sim_matrix
                , k = best_k
                , k_optimization = model_rmses
                , predicted_ratings =
                    data.table(
                      subset(
                        result[ , c( "userId", "movieId", "k", "pred" )
                              ]
                        , result$k == best_k
                        )[ , c( "userId", "movieId", "pred" ) ]
                      , key = c( "userId", "movieId" ) ) ) )
}
##########################################################




## ############################################################
## 'get_pca_optimization' function                           ##
###############################################################
# optimization procedure for the hyperparameter "prcomp_rank" #
###############################################################
# inputs :                                                    #
#    - "train_rating_residuals_matrix" similarity matrix      #
#      for the "residual ratings"                             #
#      (remainder after "Regularization")                     #
#      on the "training" dataset                              #
#    - "prcomp_ranks" list of different values                #
#      to be considered for "prcomp_ranks"                    #
#      (primary components count)                             #
#    - "test_set" data points used                            #
#      to compare predictions against 'true ratings'          #
#    - "mu_hat", "movie_avgs" and "user_avgs"                 #
#      regularization parameters                              #
#    - "print_comments" do (or not) show                      #
#      progress info on the console                           #
###############################################################
# resultset (list of objects) :                               #
#    - "pca" - the primary components decomposition object    #
#      (of class "prcomp")                                    #
#    - "prcomp_rank" - the optimum parameter value            #
#    - "prcomp_rank_optimization"                             #
#      the "RMSE" versus "prcomp_rank" dataset                #
#    - "predicted_ratings"                                    #
#      the optimized predictions in tidy format               #
#      with colnames "userId", "movieId" and "pred"           #
###############################################################
get_pca_optimization <- function(
  train_rating_residuals_matrix
  , pca = NULL
  , prcomp_ranks

  , test_set

  , mu_hat, movie_avgs, user_avgs

  , print_comments = FALSE
) {
  t2 <- proc.time()

  optimization_mode <- ( "rating" %in% colnames( test_set ) )
  if( !optimization_mode ) prcomp_ranks[ 1 ] -> prcomp_ranks

  if( is.null( pca ) ) {
    if( print_comments ) cat(
      "PCA - performing the primary components decomposition" )
    # Dimensions reduction
    # To compute the Primary Component decomposition, we
    # make the "residuals" with "NA"s equal to "0":
    non_na_residuals_matrix <- train_rating_residuals_matrix
    0 -> non_na_residuals_matrix[
      is.na( non_na_residuals_matrix ) ]
    if( print_comments ) cat( ".." )
    # save memory, set max Primary Components count
    pca <- prcomp( non_na_residuals_matrix, rank. = max( prcomp_ranks ) )
    if( print_comments ) cat( paste0(
      " done (", duration_string( t2 ), ").\n"
      , "Intermediary housekeeping - garbage collection\n" ) )
    rm( non_na_residuals_matrix )
    gc( reset = FALSE, full = TRUE )
  }

  #######################################################
  # cross validation to establish optimal 'rank.' value #
  #######################################################
  if( print_comments ) cat(
    ifelse( optimization_mode
            , "PCA - optimizing against the amount of Primary Components:\n"
            , "Generating the pca predictions :\n" ) )
  models_ranks_rmses <- sapply( seq_along( prcomp_ranks ), function( i ) {
    t1 <- proc.time()
    if( print_comments ) cat( paste0(
      "\t|"
      , paste( rep( "=", 9 ), collapse = "" )
      , "| measurement ", i, "/", length( prcomp_ranks )
      , "\n\t|" ) )
    modeled_residuals_matrix <-
      data.frame( pca$x[ , 1:prcomp_ranks[ i ] ] %*%
                    t( pca$rotation[ , 1:prcomp_ranks[ i ] ] ) )
    if( print_comments ) cat( "-" )
    rownames( modeled_residuals_matrix ) <- rownames( pca$x )
    colnames( modeled_residuals_matrix ) <- rownames( pca$rotation )
    #
    # Fast and efficient way to expand a dataset in R 
    # (tidying the matrix of residual ratings) :
    # @see 'https://stackoverflow.com/questions/44747009/fast-and-efficient-way-to-expand-a-dataset-in-r#44755588'
    data.table::setDT(  modeled_residuals_matrix
                        , keep.rownames = "userId"
    )
    modeled_residuals_matrix[ , userId := as.numeric( userId )
                              ] -> modeled_residuals_matrix
    if( print_comments ) cat( "-" )
    data.table::setkey( modeled_residuals_matrix, "userId" )
    modeled_residuals <-
      data.table::melt(
        modeled_residuals_matrix
        , id.vars = "userId"
        , variable = "movieId"
        , variable.factor = FALSE
        #, convert = TRUE
        , value.name = "residual"
      )
    if( print_comments ) cat( "-" )
    modeled_residuals[ , movieId := as.numeric( movieId ) ] -> modeled_residuals
    if( print_comments ) cat( "-" )
    data.table::setkeyv( modeled_residuals_matrix, c( "userId", "userId" ) )
    if( print_comments ) cat( "-" )
    #
    # Efficiency in Joining Two Data Frames :
    # @see 'https://www.r-bloggers.com/efficiency-in-joining-two-data-frames/'
    predicted_ratings <-
      test_set %>%
      left_join( movie_avgs, by = "movieId" ) %>%
      left_join( user_avgs, by = "userId" ) %>%
      data.table( key = c( "userId", "movieId" ) )
    if( print_comments ) cat( "-" )
    if( optimization_mode ) {
      predicted_ratings <-
        modeled_residuals[
          predicted_ratings
          , list( movieId, userId
                  , "prcomp_rank" = prcomp_ranks[ i ]
                  , "rating" = predicted_ratings$rating
                  , "b_i_hat" = predicted_ratings$b_i_hat
                  , "b_u_hat" = predicted_ratings$b_u_hat
                  , residual )
          , on = c( "userId", "movieId" )
          #, nomatch = 0
          ][ , pred :=  mu_hat + b_i_hat + b_u_hat + residual ]
    } else {
      predicted_ratings <-
        modeled_residuals[
          predicted_ratings
          , list( movieId, userId
                  , "prcomp_rank" = prcomp_ranks[ i ]
                  , "b_i_hat" = predicted_ratings$b_i_hat
                  , "b_u_hat" = predicted_ratings$b_u_hat
                  , residual )
          , on = c( "userId", "movieId" )
          #, nomatch = 0
          ][ , pred :=  mu_hat + b_i_hat + b_u_hat + residual ]
    }
    if( print_comments ) cat( "-" )
    #
    if( optimization_mode ) {
      rank_RMSE <-
        data.frame( prcomp_rank = prcomp_ranks[ i ]
                    , RMSE = RMSE( predicted_ratings$rating
                                   , predicted_ratings$pred ) )
    } else {
      rank_RMSE <- NULL
    }
    if( print_comments ) cat( "-" )
    rm( modeled_residuals_matrix, modeled_residuals )
    gc( reset = FALSE, full = TRUE )
    if( print_comments ) cat( "-" )
    #
    if( print_comments ) cat( paste0( "| done (", duration_string( t1 ), ").\n" ) )
    return( list( predicted_ratings =
                    data.table( predicted_ratings
                                , key = "prcomp_rank" )
                  , rank_RMSE = rank_RMSE
                  , elapsed = ( proc.time() - t1 )[ "elapsed" ] ) )
  } )
  #######################################################
  models_ranks_rmses <- data.frame( t( models_ranks_rmses ) )
  if( optimization_mode ) {
    prcomp_rank_optimization <-
      data.frame( do.call( rbind, t( models_ranks_rmses$rank_RMSE ) ) )
    optimized_index <- which.min( prcomp_rank_optimization$RMSE )
    #print(optimized_index)
    best_prcomp_rank <-
      prcomp_rank_optimization$prcomp_rank[ optimized_index ]
  } else {
    prcomp_rank_optimization <- NULL
    best_prcomp_rank <- prcomp_ranks
  }
  
  # models_ranks_predicted_ratings <-
  #   unnest( data.table( models_ranks_rmses$predicted_ratings ) )
  models_ranks_predicted_ratings <-
    data.frame( do.call( rbind, t( models_ranks_rmses$predicted_ratings ) ) )
  predicted_ratings <-
    data.table(
      subset(
        models_ranks_predicted_ratings[
          , c( "userId", "movieId", "prcomp_rank", "pred" ) ]
        , models_ranks_predicted_ratings$prcomp_rank == best_prcomp_rank
      )[ , c( "userId", "movieId", "pred" ) ]
      , key = c( "userId", "movieId" ) )
  
  
  if( optimization_mode & print_comments ) cat( paste(
    "Primary components optimization total duration :"
    , duration_string( t2 ), "\n" ) )
  rm( t2 )

  return( list( 
    pca = pca
    , prcomp_rank = best_prcomp_rank
    , prcomp_rank_optimization = prcomp_rank_optimization
    , predicted_ratings = predicted_ratings ) )
}
###############################################################




## ##############################################
## 'get_model_instance' function               ##
#################################################
# optimization procedure for the set of         #
# hyperparameters of our recommender system,    #
# which consists in Regularization, KNN and PCA #
#  => "lambda", "k" and "prcomp_rank"           #
#################################################
# inputs :                                      #
#    - "dataset" - the source data :            #
#      user/movie ratings                       #
#      to be provided in tidy format            #
#      with the following column names :        #
#         o "userId", "movieId", "rating"       #
#    - "dataset_test_idx"                       #
#      row numbers of 'dataset'                 #
#      to be considered for testing             #
#      (the remainder for training)             #
#    - "lambdas" list of different values       #
#      to be considered for "lambda"            #
#      (penalty term)                           #
#    - "ks" list of different values            #
#      to be considered for "k"                 #
#      (neighbors count)                        #
#    - "prcomp_ranks" list of different values  #
#      to be considered for "prcomp_rank"       #
#      (primary components count)               #
#    - "rel_props" list of different values     #
#      to be considered for "rel_prop"          #
#      (relative proportion on the final result #
#       of the weight PCA predictions over      #
#       those of the KNN prediction)            #
#    - "print_comments" do (or not) show        #
#      progress info on the console             #
#################################################
# resultset (list of objects) :                 #
#                                               #
#    - "lambda" - the optimum parameter value   #
#    - "mu_hat", "movie_avgs" and "user_avgs"   #
#      the regularization parameters            #
#    - "residuals_matrix" - "residual ratings"  #
#      (remainder after "Regularization")       #
#                                               #
#    - "similarity_matrix"                    #
#      the movies cosine similarity matrix      #
#    - "k" - the optimum parameter value        #
#    - "k_optimization"                         #
#      the "RMSE" versus "k" dataset            #
#                                               #
#    - "pca" - the primary components           #
#      decomposition object                     #
#      (of class "prcomp")                      #
#    - "prcomp_rank"                            #
#      the optimum parameter value              #
#    - "prcomp_rank_optimization"               #
#      the "RMSE" versus "prcomp_rank" dataset  #
#                                               #
#    - "rel_prop"                               #
#      the optimum parameter value              #
#    - "rel_prop_optimization"                  #
#      the "RMSE" versus "rel_prop" dataset     #
#                                               #
#    - "predicted_ratings"                      #
#      the optimized predictions                #
#      in tidy format  with colnames            #
#      "userId", "movieId" and "pred"           #
#                                               #
#################################################
get_model_instance <- function(
  dataset
  , dataset_test_idx = NULL
  , lambdas
  , ks
  , prcomp_ranks
  , rel_props
  , validation_set = NULL
  , print_comments = FALSE
) {

  print_comments_bool <-
    ( class( print_comments ) != "logical" ) || print_comments

  optimization_mode = !is.null( dataset_test_idx )
  if( optimization_mode ) {

    train_set <- dataset[ -dataset_test_idx, ]
    if( print_comments_bool ) cat( "." )

    # Ensuring that no "new" user and/or movie
    # is included in the test set =>
    # (whenever it occurs, move them into the training set
    # so as to ensure PCA matrices dimensions predictability)
    temp <- dataset[ dataset_test_idx, ]
    if( print_comments_bool ) cat( "." )
    rm( dataset_test_idx )
    test_set <-
      temp %>% 
      semi_join( train_set, by = "movieId" ) %>%
      semi_join( train_set, by = "userId" )
    if( print_comments_bool ) cat( "." )
    removed <- anti_join( temp, test_set, by = c( "userId", "movieId" ) )
    if( print_comments_bool ) cat( "." )
    train_set <- bind_rows( train_set, removed ) # rbind( train_set, removed )
    if( print_comments_bool ) cat( ".\n" )
    rm( removed, temp )
    if( print_comments_bool ) {
      cat( paste(
        "training set rows count:"
        , format(
          nrow( train_set ) # ~90% of the entire dataset for k = 10
          , nsmall = 1, big.mark = "," )
        , "\n"
      ) )
      cat( paste0(
        "test set rows count: "
        , format(
          nrow( test_set ) # ~remaining 10%
          , nsmall = 1, big.mark = "," )
        , " (", scales::percent( nrow( test_set ) / nrow( dataset ) )
        , " of the dataset)\n" ) )
    }
  } else {
    train_set <- dataset
    test_set <- validation_set
    rm( validation_set )
    if( "rating" %in% colnames( test_set ) ) {
      test_set[ -c( "rating" ) ] -> test_set }
    lambdas[ 1 ] -> lambdas
    ks[ 1 ] -> ks
    prcomp_ranks[ 1 ] -> prcomp_ranks
    rel_props[ 1 ] -> rel_props
  }
  movies_count <- dataset %>% distinct( movieId ) %>% nrow
  prcomp_ranks <- as.integer( unique( round( prcomp_ranks ) ) )
  prcomp_ranks <-
    prcomp_ranks[
      prcomp_ranks <= min( dataset %>% distinct( userId ) %>% nrow
                           , movies_count ) ]
  if( length( prcomp_ranks ) == 0 )
    prcomp_ranks <-
      min( dataset %>% distinct( userId ) %>% count
           , dataset %>% distinct( movieId ) %>% count )
  rm( dataset, movies_count )
  
  setDT( test_set, key = c( "userId", "movieId" ) )
  
  
  ###############################################
  # 'user' and 'movie' effects - Regularization #
  ###############################################

  regularization_optimization <-
    get_regularization_optimization(
      train_set = train_set
      , test_set = test_set
      , lambdas = lambdas
      , print_comments = print_comments_bool
    )
  
  #print( regularization_optimization$lambda_optimization )
  if( print_comments_bool ) {
    plot( regularization_optimization$lambda_optimization )
    if( class( print_comments ) != "logical" ) title( print_comments )
    process.events()
    cat( paste0(
      "Regularization - optimized \"penalty\" term : "
      , regularization_optimization$lambda
      , " - RMSE = "
      , round( min(
          regularization_optimization$lambda_optimization[ , "RMSE" ] )
          , 4 )
      , "\n" ) )
  }
  rm( lambdas )
  gc( reset = FALSE, full = TRUE )

  ###############################################

  residuals_matrix <-
    get_regularization_residuals_matrix(
      train_set = train_set
      , mu_hat =
          regularization_optimization$mu_hat
      , b_i_hat =
          regularization_optimization$movie_avgs$b_i_hat
      , b_u_hat =
          regularization_optimization$user_avgs$b_u_hat
    )

  ########################################
  # knn using 'movies' cosine similarity #
  ########################################

  knn_optimization <- get_knn_optimization(
    train_rating_residuals_matrix = residuals_matrix
    , ks = ks
    , test_set = test_set
    , print_comments = print_comments_bool
    , mu_hat = regularization_optimization$mu_hat
    , movie_avgs = regularization_optimization$movie_avgs
    , user_avgs = regularization_optimization$user_avgs
  )
  if( print_comments_bool ) {
    plot( knn_optimization$k_optimization )
    if( class( print_comments ) != "logical" ) title( print_comments )
    cat( paste0(
      "KNN - optimized \"k\" = "
      , format( knn_optimization$k
                , nsmall = 1, big.mark = "," )
      , " - RMSE = "
      , round( min( knn_optimization$k_optimization$RMSE ), 4 )
      , "\n" ) )
    process.events() }

  ########################################

  ########################################
  #      primary components analysis     #
  ########################################

  pca_optimization <- get_pca_optimization(
    train_rating_residuals_matrix = residuals_matrix
    , prcomp_ranks = prcomp_ranks
    , test_set = test_set
    , mu_hat = regularization_optimization$mu_hat
    , movie_avgs = regularization_optimization$movie_avgs
    , user_avgs = regularization_optimization$user_avgs
    , print_comments = print_comments_bool )
  #print( prcomp_rank_optimization )
  if( print_comments_bool ) {
    plot( pca_optimization$prcomp_rank_optimization )
    if( class( print_comments ) != "logical" ) title( print_comments )
    process.events()
    cat( paste0(
      "PCA - optimized \"prcomp_rank\" = "
      , format( pca_optimization$prcomp_rank
                , nsmall = 1, big.mark = "," )
      , " - RMSE = "
      , round( min( pca_optimization$prcomp_rank_optimization$RMSE ), 4 )
      , "\n" ) )
  }

  ########################################

  if( print_comments_bool ) cat( "housekeeping - garbage collection" )
  gc( reset = FALSE, full = TRUE )
  if( print_comments_bool ) cat( " done.\n" )
  
  ########################################
  #            pseudo-bagging            #
  ########################################

  t2 <- proc.time()
  result <-
    merge( knn_optimization$predicted_ratings, pca_optimization$predicted_ratings
           , by = c( "userId", "movieId" )
           , all = TRUE # full join (for speed)
    )[ , .( userId, movieId, knn_pred = pred.x, pca_pred = pred.y ) ]
  if( print_comments_bool ) cat( paste0(
    "BAGGING - Optimizing against respective model instance weight :\n|"
    , paste( rep( "=", length( rel_props ) + 1 ), collapse = "" )
    , "|\n|" ) )
  models_weighted_results <-
    sapply( seq_along( rel_props ), function( i ) {
    weighted_result <-
      ( result$knn_pred  + rel_props[ i ] * result$pca_pred ) / ( 1 + rel_props[ i ] )
    if( print_comments_bool ) cat( "-" )
    return( list( data.table(
      "userId" = result$userId
      , "movieId" = result$movieId
      , "rel_prop" = rep( rel_props[ i ], nrow( result ) )
      , "pred" = weighted_result ) ) )
  }, simplify = TRUE, USE.NAMES = TRUE )
  models_weighted_results <-
    data.frame( do.call( rbind, t( models_weighted_results ) ) )
  setDT( models_weighted_results, key = c( "userId", "movieId" ) )
  
  if( optimization_mode ) {
    columns <-
      c( "userId", "movieId", "rel_prop", "pred", "rating" )
  } else {
    columns <-
      c( "userId", "movieId", "rel_prop", "pred" )
  }
  merge( models_weighted_results, test_set
         , by = c( "userId", "movieId" )
         , all = TRUE # full join (for speed)
  )[ , columns
     , with = FALSE ] -> models_weighted_results
  setkey( models_weighted_results , "rel_prop" )
  if( print_comments_bool ) cat( paste0(
    "-| done (", duration_string( t2 ), ")\n" ) )

  if( optimization_mode ) {
    ## actually calculate models RMSEs ##
    show_progress_opt <- getOption( "dplyr.show_progress" )
    options( dplyr.show_progress = FALSE )
    models_weighted_rmses <-
      models_weighted_results %>% group_by( rel_prop ) %>%
      do( ( function(x)
        data.frame( RMSE =
                      RMSE( x$rating, x$pred ) ) )( . ) )
    options( dplyr.show_progress = show_progress_opt )
    best_rel_prop <-
      models_weighted_rmses$rel_prop[
        which.min( models_weighted_rmses$RMSE ) ]

    if( print_comments_bool ) {
      plot( models_weighted_rmses )
      if( class( print_comments ) != "logical" ) title( print_comments )
      process.events()
      cat( paste0(
        "BAGGING - optimized \"rel_prop\" = "
        , format( best_rel_prop
                  , nsmall = 1, big.mark = "," )
        , " - RMSE = "
        , round( min( models_weighted_rmses$RMSE ), 3 )
        , "\n" ) )
    }
  } else {
    best_rel_prop <- rel_props
    models_weighted_rmses <- NULL
  }

  ########################################
  
  predicted_ratings <-
    data.table(
      subset(
        models_weighted_results[
          , c( "userId", "movieId", "rel_prop", "pred" ) ]
        , models_weighted_results$rel_prop == best_rel_prop
      )[ , c( "userId", "movieId", "pred" ) ]
      , key = c( "userId", "movieId" ) )

  return( list( mu_hat = regularization_optimization$mu_hat

                , lambda =
                    regularization_optimization$lambda
                , movie_avgs =
                    regularization_optimization$movie_avgs
                , user_avgs =
                    regularization_optimization$user_avgs

                , residuals_matrix = residuals_matrix

                , similarity_matrix =
                    knn_optimization$similarities_matrix
                , k = knn_optimization$k

                , pca = pca_optimization$pca
                , prcomp_rank = pca_optimization$prcomp_rank

                , rel_prop = best_rel_prop

                , predicted_ratings = predicted_ratings

                , lambda_optimization =
                    regularization_optimization$lambda_optimization
                , k_optimization =
                    knn_optimization$k_optimization
                , prcomp_rank_optimization =
                    pca_optimization$prcomp_rank_optimization
                , rel_prop_optimization = models_weighted_rmses
  ) )
}
#################################################



if( FALSE ) {
  ## Not run:
  ## example of call to the "get_model_instance" function
  ## (applied to fold "f" of the 'dev_subset' data subset) :
  t1 <- proc.time() ; suppressWarnings( rm( a_model) )
  f <- 1
  a_model <- get_model_instance(
    dataset = dev_subset
    , dataset_test_idx = dev_folds[[ f ]]
    , lambdas = seq( 0, 6, .1 ) # 0 # 
    , ks = c( 2, seq( 50, 5500, by = 50 ) ) # 2 # c( 2, 50 ) # 
    , prcomp_ranks = c( 1, seq( 5, 15 ), seq( 20, 30, by = 5 ) ) # 1 # c( 1, 2 ) #
    , rel_prop = c( seq( .1, 1, by = .1 ), seq( 1.5, 6, by = .5 ), seq( 7, 10 ) ) # 1 # 
    , print_comments = TRUE )
  cat( paste( "total model training duration:", duration_string( t1 ), "\n\n" ) ) ; rm( t1 )
  ## End(Not run)
}
if( FALSE ) {
  ## Not run:
  ## example of call to the "get_model_instance" function
  ## (applied to fold "f" of the dataset) :
  t1 <- proc.time() ; suppressWarnings( rm( a_model) )
  f <- 10
  a_model <- get_model_instance(
    dataset = edx
    , dataset_test_idx = folds[[ f ]]
    , lambdas = seq( 0, 6, .1 )
    , ks = c( 2, seq( 50, 2000, by = 50 ), seq( 2000, 2500, by = 10 ) )
    , prcomp_ranks = c( 1, 10, 20, 40, seq( 47, 55, 1 ), 70, 85 )
    , rel_prop = c( seq( .1, 1, by = .1 ), seq( 1.5, 6, by = .5 ), seq( 7, 10 ) ) # 1 # 
    , print_comments = TRUE )
  cat( paste( "total model training duration:", duration_string( t1 ), "\n\n" ) ) ; rm( t1 )
  ## End(Not run)
}







## ##############################################
## 'get_cv_model_instance' function           ##
#################################################
#   a "cross-validated" mixed model instance    #
#  (k-fold validation on "regularized knn/pca   #
#               weighted" modelling)            #
#################################################
# inputs :                                      #
#    - "dataset" - the source data :            #
#      user/movie ratings                       #
#      to be provided in tidy format            #
#      with the following column names :        #
#         o "userId", "movieId", "rating"       #
#    - "cv_folds_count"                         #
#      how many folds are to be used            #
#      for hyperparameters optimization         #
#      (the remainder for training)             #
#    - "lambdas" list of different values       #
#      to be considered for "lambda"            #
#      (penalty term)                           #
#    - "ks" list of different values            #
#      to be considered for "k"                 #
#      (neighbors count)                        #
#    - "prcomp_ranks" list of different values  #
#      to be considered for "prcomp_rank"       #
#      (primary components count)               #
#    - "rel_props" list of different values     #
#      to be considered for "rel_prop"          #
#      (relative proportion on the final result #
#       of the weight PCA predictions over      #
#       those of the KNN prediction)            #
#    - "print_comments" do (or not) show        #
#      progress info on the console             #
#################################################
# resultset (list of objects) :                 #
#                                               #
#    - "lambda" - the optimum parameter value   #
#    - "mu_hat", "movie_avgs" and "user_avgs"   #
#      the regularization parameters            #
#    - "residuals_matrix" - "residual ratings"  #
#      (remainder after "Regularization")       #
#                                               #
#    - "k" - the optimum parameter value        #
#    - "similarities_matrix"                    #
#      the movies cosine similarity matrix      #
#                                               #
#    - "prcomp_rank"                            #
#      the optimum parameter value              #
#    - "pca" - the primary components           #
#      decomposition object                     #
#      (of class "prcomp")                      #
#                                               #
#    - "rel_prop"                               #
#      the optimum parameter value              #
#                                               #
#    - "cv_k_models"                            #
#      a data.frame of "cv_folds_count" rows,   #
#      each corresponding to a "fold" model     #
#      (e.g. having the structure of an object  #
#       returned by the "get_model_instance"    #
#       function)                               #
#################################################
get_cv_model_instance <- function(
  dataset
  , cv_folds_count
  , lambdas
  , ks
  , prcomp_ranks
  , rel_props
  , print_comments = FALSE
) {

  t1 <- proc.time()
  if( print_comments ) cat( paste0(
    format( dataset %>% distinct( userId ) %>% nrow
            , nsmall = 1, big.mark = "," )
    , " users ; "
    , format( dataset %>% distinct( movieId ) %>% nrow
              , nsmall = 1, big.mark = "," )
    , " movies.\n" ) )
  folds <- createFolds( y = dataset$rating
                        , k = cv_folds_count
                        , list = TRUE
                        , returnTrain = FALSE )

  ##############################
  # For each of the k-folds => #
  ##############################
  cv_k_models <- data.frame()
  #for( k in 7:9 ) {
  for( k in 1:cv_folds_count ) {
    t2 <- proc.time()
    if( print_comments ) cat( paste0(
        paste( rep( "/", 62 ), collapse = "" )
        , "\n"
        , "TRAINING FOLD #", k, "/", cv_folds_count ) )
    a_model <-
      get_model_instance(
        dataset = dataset
        , dataset_test_idx = folds[[ k ]]
        , lambdas = lambdas
        , ks = ks
        , prcomp_ranks = prcomp_ranks
        , rel_props = rel_props
        , print_comments = ifelse( print_comments, paste0( "fold_", k ), FALSE ) )
    # data.frame within data.frame
    # @see 'https://stackoverflow.com/questions/32957049/expand-data-frames-inside-data-frame'
    cv_k_models <-
      bind_rows(
        cv_k_models
        , tibble(
          cv_fold_name = paste0( "fold_", substring( paste0( "0", k )
                                                     , nchar( paste0( "0", k ) ) - 2 + 1 ) )
          , lambda = a_model$lambda
          , mu_hat = a_model$mu_hat
          , user_avgs = list( data.frame( a_model$user_avgs ) )
          , movie_avgs = list( data.frame( a_model$movie_avgs ) )
          # , residuals_matrix = list( a_model$residuals_matrix )

          , k = a_model$k
          # , similarities_matrix = list( a_model$similarities_matrix )

          , prcomp_rank = a_model$prcomp_rank
          , pca = list( a_model$pca )

          , rel_prop = a_model$rel_prop

          , lambda_optimization = list( data.frame( a_model$lambda_optimization ) )
          , k_optimization = list( data.frame( a_model$k_optimization ) )
          , prcomp_rank_optimization = list( data.frame( a_model$prcomp_rank_optimization ) )
          , rel_prop_optimization = list( data.frame( a_model$rel_prop_optimization ) )
        ) )
    rm( a_model ) ; gc( reset = FALSE, full = TRUE )
    if( print_comments ) cat( paste0(
      "fold #", k, "/", cv_folds_count, " training duration : "
      , duration_string( t2 ) , ".\n\n" ) )
  }
  if( print_comments ) {
    grid.arrange(
      cv_k_models %>% select( cv_fold_name, lambda_optimization ) %>% unnest %>%
        ggplot( aes( x = lambda, y = RMSE, color = cv_fold_name ) ) +
        geom_line( show.legend = FALSE ) +
        geom_vline( xintercept = mean( cv_k_models$lambda )
                    , size = 1, color = "skyblue3", linetype = "dashed" )
      , cv_k_models %>% select( cv_fold_name, k_optimization ) %>% unnest %>%
        ggplot( aes( x = k, y = RMSE, color = cv_fold_name ) ) + geom_line() +
        geom_vline( xintercept = mean( cv_k_models$k )
                    , size = 1, color = "skyblue3", linetype = "dashed" ) +
        guides( color = guide_legend( ncol = 2 ) ) +
        theme( legend.justification = c(1, 1), legend.position = c(1, 1) )
      , cv_k_models %>% select( cv_fold_name, prcomp_rank_optimization ) %>% unnest %>%
        ggplot( aes( x = prcomp_rank, y = RMSE, color = cv_fold_name ) ) +
        geom_line( show.legend = FALSE ) +
        geom_vline( xintercept = mean( cv_k_models$prcomp_rank )
                    , size = 1, color = "skyblue3", linetype = "dashed" )
      , cv_k_models %>% select( cv_fold_name, rel_prop_optimization ) %>% unnest %>%
        ggplot( aes( x = rel_prop, y = RMSE, color = cv_fold_name ) ) +
        geom_line( show.legend = FALSE ) +
        geom_vline( xintercept = mean( cv_k_models$rel_prop )
                    , size = 1, color = "skyblue3", linetype = "dashed" )
      , nrow = 4 )
    process.events()
    cat( paste0( 
    paste( rep( "/", 62 ), collapse = "" )
    , "\n"
    , "total k-fold model instances training duration : ", duration_string( t1 )
    , "\n"
    , paste( rep( "/", 62 ), collapse = "" )
    , "\n\n" ) )
  }
  rm( t1, k, folds ) ; gc( reset = FALSE, full = TRUE )
  
  ##########################
  # drawing conclusions => #
  ##########################

  lambda <- cv_k_models$lambda %>% mean
  regularization_optimization <-
    get_regularization_optimization(
      train_set = dataset
      , test_set = NULL
      , lambdas = lambda
      , print_comments = print_comments
    )
  mu_hat <-
    regularization_optimization$mu_hat
  user_avgs <-
    regularization_optimization$user_avgs
  movie_avgs <-
    regularization_optimization$movie_avgs
  rm( regularization_optimization )

  residuals_matrix <-
    get_regularization_residuals_matrix(
      train_set = dataset
      , mu_hat = mu_hat
      , b_u_hat = user_avgs$b_u_hat
      , b_i_hat = movie_avgs$b_i_hat
    )

  k <- cv_k_models$k %>% mean %>% round( digits = 0 )
  t1 <- proc.time()
  cat( "KNN - Computing the movies cosine similarity matrix.." )
  similarity_matrix <-
    coop::cosine( residuals_matrix, use = "pairwise.complete" )
  similarity_matrix - diag( ncol( similarity_matrix ) ) ->
    similarity_matrix
  cat( paste0( " done (", duration_string( t1 ), ")\n" ) )

  prcomp_rank = cv_k_models$prcomp_rank %>% mean %>% ceiling
  t1 <- proc.time()
  if( print_comments ) cat(
    "PCA - performing the primary components decomposition" )
  # Dimensions reduction
  # To compute the Primary Component decomposition, we
  # make the "residuals" with "NA"s equal to "0":
  non_na_residuals_matrix <- residuals_matrix
  0 -> non_na_residuals_matrix[
    is.na( non_na_residuals_matrix ) ]
  if( print_comments ) cat( ".." )
  # save memory, set max Primary Components count
  pca <- prcomp( non_na_residuals_matrix, rank. = prcomp_rank )
  if( print_comments ) cat( paste0(
    " done (", duration_string( t1 ), ").\n" ) )

  rm( non_na_residuals_matrix, t1 )
  gc( reset = FALSE, full = TRUE )

  return( list(
    lambda = lambda
    , mu_hat = mu_hat
    , user_avgs = user_avgs
    , movie_avgs = movie_avgs
    , residuals_matrix = residuals_matrix

    , k = k
    , similarity_matrix = similarity_matrix

    , prcomp_rank = prcomp_rank
    , pca = pca

    , rel_prop =  cv_k_models$rel_prop %>% mean

    , cv_k_models = cv_k_models ) )
}
#################################################


if( FALSE ) {
  ## Not run:
  ## example of call to the "get_cv_model_instance" function
  ## (applied to the 'dev_subset' data subset) :
  set.seed( 1806 ) # for reproduceability
  dev_trained_model_instance <- get_cv_model_instance(
    dataset = dev_subset
    , cv_folds_count = 10
    , lambdas = seq( 0, 6, .1 ) # 0 # 
    , ks = c( 2, seq( 50, 5500, by = 50 ) ) # 2 # 
    , prcomp_ranks = c( 1, seq( 5, 15 ), seq( 20, 30, by = 5 ) ) # 1 # 
    , rel_props = c( seq( .1, 1, by = .1 ), seq( 2, 4, by = .1 ), seq( 5, 10 ) ) # 1 # 
    , print_comments = TRUE )
  cat( "serializing result.." ) ; saveRDS( dev_trained_model_instance, "dev_trained_model_instance" ) ; cat( "\n" )
  # nrow( dev_trained_model_instance$cv_k_models ) ; names( dev_trained_model_instance$cv_k_models )
  # names( dev_trained_model_instance )
  # str( dev_trained_model_instance )
  ## End(Not run)
}
if( FALSE ) {
  ## Not run:
  ## example of call to the "get_cv_model_instance" function :
  set.seed( 1806 ) # for reproduceability
  trained_model_instance <- get_cv_model_instance(
    dataset = edx
    , cv_folds_count = 10 
    , lambdas = seq( 0, 6, .1 ) # 0 # 
    , ks = c( 2, seq( 50, 2000, by = 50 ), seq( 2000, 2500, by = 10 ) ) # 2 # 
    , prcomp_ranks = c( 1, 10, 20, 40, seq( 47, 55, 1 ), 70, 85 ) # 1 # 
    , rel_props = c( seq( .1, 1, by = .1 ), seq( 2, 4, by = .1 ), seq( 5, 10 ) ) # 1 # 
    , print_comments = TRUE )
  cat( "serializing result.." ) ; saveRDS( trained_model_instance, "trained_model_instance" ) ; cat( "\n" )
  # nrow( trained_model_instance$cv_k_models ) ; names( trained_model_instance$cv_k_models )
  # names( trained_model_instance )
  # str( trained_model_instance )
  ## End(Not run)
}




## ######################################################
## 'model_predict' function                            ##
#########################################################
# inputs :                                              #
#    - "trained_model_instance" - a 'cross-validated'   #
#      trained model instance as generated              #
#      by the 'get_cv_model_instance' function          #
#    - "validation_set" - the 'validation'              #
#      dataset                                          #
#      to be provided in tidy format                    #
#      with the following column names :                #
#         o "userId", "movieId"                         #
#    - "true_ratings" ordered vector of 'true ratings'  #
#      for the "validation_set" list of observations    #
#      (optional, if RMSE measures are to be provided). #
#    - "print_comments" do (or not) show                #
#      progress info on the console                     #
#########################################################
# resultset : the predicted ratings                     #
#########################################################
model_predict <- function(
  trained_model_instance
  , validation_set

  , true_ratings = NULL

  , print_comments = FALSE
) {
  t1 <- proc.time()
  if( print_comments ) cat( paste0(
    format( validation_set %>% distinct( userId ) %>% nrow
            , nsmall = 1, big.mark = "," )
    , " users ; "
    , format( validation_set %>% distinct( movieId ) %>% nrow
              , nsmall = 1, big.mark = "," )
    , " movies.\n" ) )

  
  k <- trained_model_instance$k
  mu_hat <- trained_model_instance$mu_hat
  user_avgs <- trained_model_instance$user_avgs
  movie_avgs <- trained_model_instance$movie_avgs
  similarity_matrix <- trained_model_instance$similarity_matrix
  residuals_matrix <- trained_model_instance$residuals_matrix
  
  prcomp_rank <- trained_model_instance$prcomp_rank
  pca <- trained_model_instance$pca

  rel_prop <- trained_model_instance$rel_prop
  rm( trained_model_instance )

  knn_predictions <- get_knn_optimization(
    ks = k
    , train_rating_residuals_matrix = residuals_matrix
    , mu_hat = mu_hat
    , user_avgs = user_avgs
    , movie_avgs = movie_avgs
    , train_sim_matrix = similarity_matrix
    , test_set = validation_set
    , print_comments = print_comments
  )$predicted_ratings
  rm( similarity_matrix )

  pca_predictions <- get_pca_optimization(
    train_rating_residuals_matrix = residuals_matrix
    , pca = pca
    , prcomp_ranks = prcomp_rank
    , test_set = validation_set
    , mu_hat = mu_hat
    , user_avgs = user_avgs
    , movie_avgs = movie_avgs
    , print_comments = print_comments
  )$predicted_ratings
  rm( residuals_matrix, pca, mu_hat, user_avgs, movie_avgs )
  
  weighted_predictions <-
    ( knn_predictions$pred  + rel_prop * pca_predictions$pred ) / ( 1 + rel_prop )

  if( !is.null( true_ratings ) ) {
    prediction_RMSE <- data.table(
      "regularized + knn" = RMSE( true_ratings, knn_predictions$pred )
      , "regularized + pca" = RMSE( true_ratings, pca_predictions$pred )
      , weighted = RMSE( true_ratings, weighted_predictions )
    )
  } else { prediction_RMSE <- NULL }

  result <- list(
    predictions = data.frame(
      userId = knn_predictions$userId
      , movieId = knn_predictions$movieId
    
      , knn_predictions = knn_predictions$pred
      , pca_predictions = pca_predictions$pred
      , weighted_predictions = weighted_predictions
    
      , knn_to_rating = mapply( knn_predictions$pred
                                , FUN = to_rating )
      , pca_to_rating = mapply( pca_predictions$pred
                                , FUN = to_rating )
      , weighted_to_rating = mapply( weighted_predictions
                                     , FUN = to_rating )
    )

    , RMSE = prediction_RMSE
  )

  if( print_comments ) cat( paste0(
    "prediction duration : ", duration_string( t1 ) ) )
  return( result )
}
#########################################################




if( FALSE ) {
  ## Not run:
  ## example of call to the "model_predict" function
  ## (applied to the 'dev_subset' data subset) :
  dev_predictions <-
    model_predict(
      trained_model_instance = dev_trained_model_instance
      , validation_set =
          dev_validation[ , c( "userId", "movieId" ) ]
      , true_ratings = dev_validation[ , c( "rating" ) ]
      , print_comments = TRUE )
  cat( "serializing result.." ) ; saveRDS( dev_predictions, "dev_predictions" ) ; cat( "\n" )
  write.csv( dev_predictions$predictions %>%
               select( userId, movieId, weighted_predictions
               ) %>% rename( rating = weighted_predictions )
             , "dev_submission.csv", row.names = FALSE )
  View( bind_cols( dev_predictions$predictions
                   , knn_pca =
                     ( dev_predictions$predictions$knn_to_rating ==
                         dev_predictions$predictions$pca_to_rating )
                   , pca_weighted =
                     ( dev_predictions$predictions$pca_to_rating ==
                         dev_predictions$predictions$weighted_to_rating ) )
        , "predictions" )

  if( !require( generics ) ) { install.packages( "generics" ) }
  library( generics )
  dev_confusion_matrix <-
    bind_cols(
      generics::tidy(
        confusionMatrix(
          data = factor( dev_predictions$predictions$knn_to_rating
                         , levels = seq( .5, 5, by = .5 ) )
          , reference = factor( dev_validation$rating
                                , levels = seq( .5, 5, by = .5 )
          ) ) ) %>%
        filter(
          term %in%
            c( "accuracy", "sensitivity", "specificity"
            ) ) %>%
        select( term, class, estimate ) %>%
        rename( "knn_estimate" = "estimate" )
      , generics::tidy(
        confusionMatrix(
          data = factor( dev_predictions$predictions$pca_to_rating
                         , levels = seq( .5, 5, by = .5 ) )
          , reference = factor( dev_validation$rating
                                , levels = seq( .5, 5, by = .5 )
          ) ) ) %>%
        filter(
          term %in%
            c( "accuracy", "sensitivity", "specificity"
            ) ) %>%
        select( estimate ) %>%
        rename( "pca_estimate" = "estimate" )
      , generics::tidy(
        confusionMatrix(
          data = factor( dev_predictions$predictions$weighted_to_rating
                         , levels = seq( .5, 5, by = .5 ) )
          , reference =
            factor( dev_validation$rating
                    , levels = seq( .5, 5, by = .5 )
            ) ) ) %>%
        filter(
          term %in%
            c( "accuracy", "sensitivity", "specificity"
            ) ) %>%
        select( estimate ) %>%
        rename( "weighted_estimate" = "estimate" )
    )
  # low sensitivity, high specificity
  knitr::kable( dev_confusion_matrix %>% arrange( class )
                , digits = 3, format = "rst" )

  # how many times do the "knn" and the "pca" predictions differ ?
  sum( ( dev_predictions$predictions$knn_to_rating ==
           dev_predictions$predictions$pca_to_rating ) )

  # how many times does the "weighted" result
  # correspond to the "knn" prediction ?
  nrow( dev_predictions$predictions ) -
    sum( ( dev_predictions$predictions$pca_to_rating ==
             dev_predictions$predictions$weighted_to_rating ) )
  # how many times does the "weighted" result
  # correspond to the "pca" prediction ?
  sum( ( dev_predictions$predictions$pca_to_rating ==
           dev_predictions$predictions$weighted_to_rating ) )

  dev_predictions$RMSE
  ## End(Not run)
}

if( FALSE ) {
  ## Not run:
  ## example of call to the "model_predict" function :
  predictions <-
    model_predict( trained_model_instance = trained_model_instance
                   , validation_set =
                      validation[ , c( "userId", "movieId" ) ]
                   , true_ratings =
                      validation[ , c( "rating" ) ]
                   , print_comments = TRUE )
  cat( "serializing result.." ) ; saveRDS( predictions, "predictions" ) ; cat( "\n" )
  write.csv( predictions$predictions %>%
               select( userId, movieId, weighted_predictions
               ) %>% rename( rating = weighted_predictions )
             , "submission.csv", row.names = FALSE )
  View( bind_cols( predictions$predictions
                   , knn_pca =
                     ( predictions$predictions$knn_to_rating ==
                         predictions$predictions$pca_to_rating )
                   , pca_weighted =
                     ( predictions$predictions$pca_to_rating ==
                         predictions$predictions$weighted_to_rating ) )
        , "predictions" )
  
  if( !require( generics ) ) { install.packages( "generics" ) }
  library( generics )
  confusion_matrix <-
    bind_cols(
      generics::tidy(
        confusionMatrix(
          data = factor( predictions$predictions$knn_to_rating
                         , levels = seq( .5, 5, by = .5 ) )
          , reference =
            factor( validation$rating
                    , levels = seq( .5, 5, by = .5 ) ) ) ) %>%
        filter(
          term %in%
            c( "accuracy", "sensitivity", "specificity"
            ) ) %>%
        select( term, class, estimate ) %>%
        rename( "knn_estimate" = "estimate" )
      , generics::tidy(
        confusionMatrix(
          data = factor( predictions$predictions$pca_to_rating
                         , levels = seq( .5, 5, by = .5 ) )
          , reference =
            factor( validation$rating
                    , levels = seq( .5, 5, by = .5 ) ) ) ) %>%
        filter(
          term %in%
            c( "accuracy", "sensitivity", "specificity"
            ) ) %>%
        select( estimate ) %>%
        rename( "pca_estimate" = "estimate" )
      , generics::tidy(
        confusionMatrix(
          data = factor( predictions$predictions$weighted_to_rating
                         , levels = seq( .5, 5, by = .5 ) )
          , reference =
            factor( validation$rating
                    , levels = seq( .5, 5, by = .5 ) ) ) ) %>%
        filter(
          term %in%
            c( "accuracy", "sensitivity", "specificity"
            ) ) %>%
        select( estimate ) %>%
        rename( "weighted_estimate" = "estimate" )
    )
  # low sensitivity, high specificity
  knitr::kable( confusion_matrix %>% arrange( class )
                , digits = 3, format = "rst" )
  ?knitr::kable
  # how many times do the "knn" and the "pca" predictions differ ?
  sum( ( predictions$predictions$knn_to_rating ==
           predictions$predictions$pca_to_rating ) )
  
  # how many times does the "weighted" result
  # correspond to the "knn" prediction ?
  nrow( predictions$predictions ) -
    sum( ( predictions$predictions$pca_to_rating ==
             predictions$predictions$weighted_to_rating ) )
  # how many times does the "weighted" result
  # correspond to the "pca" prediction ?
  sum( ( predictions$predictions$pca_to_rating ==
           predictions$predictions$weighted_to_rating ) )
  
  predictions$RMSE
  ## End(Not run)
}








## ///////////////////////////////// EXTRA ///////////////////////////////// ##



if( FALSE ) {
  ## Not run:
  # EXTRA : how to average over a list of matrices ;
  # use case : averaging over the "k" optimized PCA "weights (x)" matrices :

  prcomp_rank <- 51
  # Processing nested lists
  # @see 'https://www.r-bloggers.com/processing-nested-lists/'
  x_matrices_list <-
    lapply( trained_model_instance$cv_k_models %>% select( pca )
            , function( pca_item ) lapply( pca_item
                                           , function( pca_item )
                                             pca_item$x[ , 1:prcomp_rank ] ) )
  # Element-wise mean over list of matrices
  # @see 'https://stackoverflow.com/questions/19218475/element-wise-mean-over-list-of-matrices#19220503'
  x_matrices_3D <-
    array( unlist( x_matrices_list )
           , c( nrow( trained_model_instance$cv_k_models$pca[[ 1 ]]$x )
                , prcomp_rank
                , 2 ) )
  rm( x_matrices_list )
  mean_x_matrix <- rowMeans( x_matrices_3D , dims = 2 )
  #rm( x_matrices_3D )
  dimnames( mean_x_matrix ) <-
    dimnames( cv_k_models$pca[[ 1 ]]$x[ , 1:prcomp_rank ] )
  #View( x_matrices_3D[ , , 1 ] ) ; View( x_matrices_3D[ , , 2 ] )
  View( mean_x_matrix )
  image( mean_x_matrix )

  dim( x_matrices_3D )
  dim( mean_x_matrix )

  # View( trained_model_instance$cv_k_models$pca[[ 1 ]] )
  # trained_model_instance$cv_k_models$pca[[ 1 ]]$center %>% min
  # trained_model_instance$cv_k_models$pca[[ 1 ]]$center %>% max
  # trained_model_instance$cv_k_models$pca[[ 1 ]]$center %>% median
  # trained_model_instance$cv_k_models$pca[[ 1 ]]$center %>% mean
  # trained_model_instance$cv_k_models$pca[[ 1 ]]$center %>% sum

  ## End(Not run)
}


if( FALSE ) {
  ## Not run:
  # EXTRA : how to average over a list of tidy datasets ;
  # use case : averaging over the "k" "movie_avgs" :
  #trained_model_instance$cv_k_models$movie_avgs ; str( trained_model_instance$cv_k_models ) ; View( trained_model_instance$cv_k_models %>% select( -pca ) )
  trained_model_instance$cv_k_models$mu_hat
  trained_model_instance$cv_k_models$lambda # { 1.70 - 5.00 }
  trained_model_instance$cv_k_models %>%
    select( cv_fold_name, movie_avgs) %>% unnest %>% View
  trained_model_instance$cv_k_models %>%
    select( cv_fold_name, movie_avgs) %>% unnest %>% View
  #
  # averaging over models =>
  #?reduce
  View(
    trained_model_instance$cv_k_models$movie_avgs %>%
      reduce( full_join, by = c("movieId") ) %>%
      mutate( b_i_hat = rowMeans( .[ , -1 ], na.rm = TRUE )
              , sd = rowSds( x = as.matrix( .[ , -1 ] ) ) ) %>%
      filter( sd >= .05 )
  )
  
  ## End(Not run)
}





# dev_trained_model_instance <- readRDS( "dev_trained_model_instance" )
# dev_predictions <- readRDS( "dev_predictions" )
# dev_trained_model_instance <- readRDS( "dev_trained_model_instance" )
# predictions <- readRDS( "predictions" )



gc( reset = FALSE, full = TRUE )





















