# Functions that retrieve data stored in PostGIS database

require("RPostgreSQL")
require("dplyr")

data_stuhr_loading <- function(){
  
  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv, dbname = "bremen",
                   host = "localhost", port = 5432,
                   user = "postgres", password = "postgres")
  stuhr <- dbGetQuery(con, "SELECT * from indicators.stuhr")
  stuhr_v <- dbGetQuery(con, "SELECT * from indicators.stuhr_v")
  stuhr_fusion.users <- dbGetQuery(con, "SELECT DISTINCT ON (B.uid) B.*
                                   FROM indicators.stuhr_fusion A
                                   LEFT JOIN indicators.users_stuhr B ON A.uid = B.uid;")
  
  
  stuhr$vandalism = FALSE
  stuhr_v$vandalism=  TRUE
  fusion <- rbind(stuhr, stuhr_v)
  rm(stuhr)
  rm(stuhr_v)
  
  # Modifying the author of type C vandalism : user #7000002
  stuhr_fusion.users$total_contributions[ which(stuhr_fusion.users$uid==7000002)]<- 120
  stuhr_fusion.users$p_is_edited[ which(stuhr_fusion.users$uid==7000002)]<- 0.25
  stuhr_fusion.users$p_is_deleted[ which(stuhr_fusion.users$uid==7000002)]<- 0.25
  stuhr_fusion.users$focalisation[ which(stuhr_fusion.users$uid==7000002)]<- 0.3
  
  # Reducing and centering the variables
  stuhr_fusion.users_scale <- as.data.frame(stuhr_fusion.users$uid) 
  names(stuhr_fusion.users_scale)[1]<-"uid"
  stuhr_fusion.users_scale$total_contributions <- scale(stuhr_fusion.users$total_contributions)
  stuhr_fusion.users_scale$p_creation <- scale(stuhr_fusion.users$p_creation)
  stuhr_fusion.users_scale$p_modification <- scale(stuhr_fusion.users$p_modification)
  stuhr_fusion.users_scale$p_delete <- scale(stuhr_fusion.users$p_delete)
  stuhr_fusion.users_scale$p_is_used <- scale(stuhr_fusion.users$p_is_used)
  stuhr_fusion.users_scale$p_is_edited <- scale(stuhr_fusion.users$p_is_edited)
  stuhr_fusion.users_scale$p_is_deleted <- scale(stuhr_fusion.users$p_is_deleted)
  stuhr_fusion.users_scale$nbWeeks <- scale(stuhr_fusion.users$nbWeeks)
  stuhr_fusion.users_scale$focalisation <- scale(stuhr_fusion.users$focalisation)
  
  
  stuhr_fusion.users_scale$avg <- (stuhr_fusion.users_scale$total_contributions + stuhr_fusion.users_scale$p_modification 
                                   + stuhr_fusion.users_scale$p_delete + stuhr_fusion.users_scale$p_is_used
                                   + stuhr_fusion.users_scale$p_is_edited + stuhr_fusion.users_scale$p_is_deleted
                                   + stuhr_fusion.users_scale$nbWeeks + stuhr_fusion.users_scale$focalisation)/8
  stuhr_fusion.users_scale$weighted_avg <- (stuhr_fusion.users_scale$total_contributions + stuhr_fusion.users_scale$p_modification 
                                            + stuhr_fusion.users_scale$p_delete + 2*stuhr_fusion.users_scale$p_is_used
                                            + 2*stuhr_fusion.users_scale$p_is_edited + 2*stuhr_fusion.users_scale$p_is_deleted
                                            + 3*stuhr_fusion.users_scale$nbWeeks + 3*stuhr_fusion.users_scale$focalisation)/15
  
  
  # Jointure des indicateurs contributeurs sur les batiments de la table fusion
  require("dplyr")
  fusion_users <- left_join(fusion, stuhr_fusion.users_scale)

  return(fusion_users)
  
}

data_aubervilliers_loading <- function(){
  
  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv, dbname = "idf",
                   host = "localhost", port = 5432,
                   user = "postgres", password = "postgres")
  
  aubervilliers <- dbGetQuery(con, "SELECT * from indicators.aubervilliers WHERE id NOT IN (SELECT id FROM indicators.aubervilliers_vandalism)")
  aubervilliers_v <- dbGetQuery(con, "SELECT * from indicators.aubervilliers_vandalism")
  aubervilliers_fusion.users <- dbGetQuery(con, "SELECT DISTINCT ON (B.uid) B.*, A.n_semesters_auberv
                                           FROM (select uid, n_semesters_auberv from indicators.aubervilliers union
                                           select uid, n_semesters_auberv from indicators.aubervilliers_vandalism) as A 
                                           LEFT JOIN indicators.users_aubervilliers B ON A.uid = B.uid;")
  
  aubervilliers$vandalism = FALSE
  aubervilliers_v$vandalism=  TRUE
  # On retire la colonne vandalism_type, qui  n'existe pas dans la table aubervilliers
  aubervilliers_v$vandalism_type <- NULL
  
  fusion <- rbind(aubervilliers, aubervilliers_v)
  rm(aubervilliers)
  rm(aubervilliers_v)
  
  # Centrage et reduction des variables
  aubervilliers_fusion.users_scale <- as.data.frame(aubervilliers_fusion.users$uid) 
  names(aubervilliers_fusion.users_scale)[1]<-"uid"
  aubervilliers_fusion.users_scale$total_contributions <- scale(aubervilliers_fusion.users$total_contributions)
  aubervilliers_fusion.users_scale$p_creation <- scale(aubervilliers_fusion.users$p_creation)
  aubervilliers_fusion.users_scale$p_modification <- scale(aubervilliers_fusion.users$p_modification)
  aubervilliers_fusion.users_scale$p_delete <- scale(aubervilliers_fusion.users$p_delete)
  aubervilliers_fusion.users_scale$p_is_used <- scale(aubervilliers_fusion.users$p_is_used)
  aubervilliers_fusion.users_scale$p_is_edited <- scale(aubervilliers_fusion.users$p_is_edited)
  aubervilliers_fusion.users_scale$p_is_deleted <- scale(aubervilliers_fusion.users$p_is_deleted)
  aubervilliers_fusion.users_scale$nbWeeks <- scale(aubervilliers_fusion.users$nbWeeks)
  aubervilliers_fusion.users_scale$focalisation <- scale(aubervilliers_fusion.users$focalisation)
  
  
  aubervilliers_fusion.users_scale$avg <- (aubervilliers_fusion.users_scale$total_contributions + aubervilliers_fusion.users_scale$p_modification 
                                           + aubervilliers_fusion.users_scale$p_delete + aubervilliers_fusion.users_scale$p_is_used
                                           + aubervilliers_fusion.users_scale$p_is_edited + aubervilliers_fusion.users_scale$p_is_deleted
                                           + aubervilliers_fusion.users_scale$nbWeeks + aubervilliers_fusion.users_scale$focalisation)/8
  aubervilliers_fusion.users_scale$weighted_avg <- (aubervilliers_fusion.users_scale$total_contributions + aubervilliers_fusion.users_scale$p_modification 
                                                    + aubervilliers_fusion.users_scale$p_delete + 2*aubervilliers_fusion.users_scale$p_is_used
                                                    + 2*aubervilliers_fusion.users_scale$p_is_edited + 2*aubervilliers_fusion.users_scale$p_is_deleted
                                                    + 3*aubervilliers_fusion.users_scale$nbWeeks + 3*aubervilliers_fusion.users_scale$focalisation)/15
  
  
  # Jointure des indicateurs contributeurs sur les batiments de la table fusion
  require("dplyr")
  fusion_users <- left_join(fusion, aubervilliers_fusion.users_scale)
  
  return(fusion_users)
  
}


data_aubervilliers_loading_non_centre_reduit <- function(){
  
  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv, dbname = "idf",
                   host = "localhost", port = 5432,
                   user = "postgres", password = "postgres")
  
  aubervilliers <- dbGetQuery(con, "SELECT * from indicators.aubervilliers WHERE id NOT IN (SELECT id FROM indicators.aubervilliers_vandalism)")
  aubervilliers_v <- dbGetQuery(con, "SELECT * from indicators.aubervilliers_vandalism")
  aubervilliers_fusion.users <- dbGetQuery(con, "SELECT DISTINCT ON (B.uid) B.*, A.n_semesters_auberv
                                           FROM (select uid, n_semesters_auberv from indicators.aubervilliers union
                                           select uid, n_semesters_auberv from indicators.aubervilliers_vandalism) as A 
                                           LEFT JOIN indicators.users_aubervilliers B ON A.uid = B.uid;")
  
  aubervilliers$vandalism = FALSE
  aubervilliers_v$vandalism=  TRUE
  # On retire la colonne vandalism_type, qui  n'existe pas dans la table aubervilliers
  aubervilliers_v$vandalism_type <- NULL
  
  fusion <- rbind(aubervilliers, aubervilliers_v)
  rm(aubervilliers)
  rm(aubervilliers_v)
  
  # Centrage et r?duction des variables
  aubervilliers_fusion.users_unscale <- as.data.frame(aubervilliers_fusion.users$uid) 
  names(aubervilliers_fusion.users_unscale)[1]<-"uid"
  aubervilliers_fusion.users_unscale$total_contributions <- aubervilliers_fusion.users$total_contributions
  aubervilliers_fusion.users_unscale$p_creation <- aubervilliers_fusion.users$p_creation
  aubervilliers_fusion.users_unscale$p_modification <- aubervilliers_fusion.users$p_modification
  aubervilliers_fusion.users_unscale$p_delete <- aubervilliers_fusion.users$p_delete
  aubervilliers_fusion.users_unscale$p_is_used <- aubervilliers_fusion.users$p_is_used
  aubervilliers_fusion.users_unscale$p_is_edited <- aubervilliers_fusion.users$p_is_edited
  aubervilliers_fusion.users_unscale$p_is_deleted <- aubervilliers_fusion.users$p_is_deleted
  aubervilliers_fusion.users_unscale$nbWeeks <- aubervilliers_fusion.users$nbWeeks
  aubervilliers_fusion.users_unscale$focalisation <- aubervilliers_fusion.users$focalisation
  
  
  aubervilliers_fusion.users_unscale$avg <- (aubervilliers_fusion.users_unscale$total_contributions + aubervilliers_fusion.users_unscale$p_modification 
                                             + aubervilliers_fusion.users_unscale$p_delete + aubervilliers_fusion.users_unscale$p_is_used
                                             + aubervilliers_fusion.users_unscale$p_is_edited + aubervilliers_fusion.users_unscale$p_is_deleted
                                             + aubervilliers_fusion.users_unscale$nbWeeks + aubervilliers_fusion.users_unscale$focalisation)/8
  aubervilliers_fusion.users_unscale$weighted_avg <- (aubervilliers_fusion.users_unscale$total_contributions + aubervilliers_fusion.users_unscale$p_modification 
                                                      + aubervilliers_fusion.users_unscale$p_delete + 2*aubervilliers_fusion.users_unscale$p_is_used
                                                      + 2*aubervilliers_fusion.users_unscale$p_is_edited + 2*aubervilliers_fusion.users_unscale$p_is_deleted
                                                      + 3*aubervilliers_fusion.users_unscale$nbWeeks + 3*aubervilliers_fusion.users_unscale$focalisation)/15
  
  
  # Jointure des indicateurs contributeurs sur les batiments de la table fusion
  require("dplyr")
  fusion_users <- left_join(fusion, aubervilliers_fusion.users_unscale)
  
  return(fusion_users)
  
}


load_data <- function(dbName, cityName){
  
  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv, dbname = dbName,
                   host = "localhost", port = 5432,
                   user = "postgres", password = "postgres")
  
  query <-  paste("SELECT * from indicators.", cityName, sep ="")
  
  city <- dbGetQuery(con, toString(query) )
  
  query <- paste(query, "_v", sep ="")
  
  city_v <- dbGetQuery(con, query)
  
  city_fusion_name <- paste("indicators.", paste(cityName,"_fusion", sep=""), sep="")
  users_fusion_name <- paste("indicators.", paste("users_",cityName, sep=""), sep="")
  
  query <- paste("SELECT DISTINCT ON (B.uid) B.* FROM ", paste(city_fusion_name, "A", sep=" "), sep="")
  query_left_join <- paste("LEFT JOIN ", paste(users_fusion_name," B ON A.uid = B.uid;", sep=""), sep ="")
  query <- paste(query, query_left_join)
  
  
  city_fusion.users <- dbGetQuery(con, query)
  
  
  city$vandalism = FALSE
  city_v$vandalism=  TRUE
  fusion <- rbind(city, city_v)
  rm(city)
  rm(city_v)
  
  # Centrage et reduction des variables
  city_fusion.users_scale <- as.data.frame(city_fusion.users$uid) 
  names(city_fusion.users_scale)[1]<-"uid"
  city_fusion.users_scale$total_contributions <- scale(city_fusion.users$total_contributions)
  city_fusion.users_scale$p_creation <- scale(city_fusion.users$p_creation)
  city_fusion.users_scale$p_modification <- scale(city_fusion.users$p_modification)
  city_fusion.users_scale$p_delete <- scale(city_fusion.users$p_delete)
  city_fusion.users_scale$p_is_used <- scale(city_fusion.users$p_is_used)
  city_fusion.users_scale$p_is_edited <- scale(city_fusion.users$p_is_edited)
  city_fusion.users_scale$p_is_deleted <- scale(city_fusion.users$p_is_deleted)
  city_fusion.users_scale$nbWeeks <- scale(city_fusion.users$nbWeeks)
  city_fusion.users_scale$focalisation <- scale(city_fusion.users$focalisation)
  
  
  city_fusion.users_scale$avg <- (city_fusion.users_scale$total_contributions + city_fusion.users_scale$p_modification 
                                  + city_fusion.users_scale$p_delete + city_fusion.users_scale$p_is_used
                                  + city_fusion.users_scale$p_is_edited + city_fusion.users_scale$p_is_deleted
                                  + city_fusion.users_scale$nbWeeks + city_fusion.users_scale$focalisation)/8
  city_fusion.users_scale$weighted_avg <- (city_fusion.users_scale$total_contributions + city_fusion.users_scale$p_modification 
                                           + city_fusion.users_scale$p_delete + 2*city_fusion.users_scale$p_is_used
                                           + 2*city_fusion.users_scale$p_is_edited + 2*city_fusion.users_scale$p_is_deleted
                                           + 3*city_fusion.users_scale$nbWeeks + 3*city_fusion.users_scale$focalisation)/15
  
  
  # Jointure des indicateurs contributeurs sur les batiments de la table fusion
  require("dplyr")
  fusion_users <- left_join(fusion, city_fusion.users_scale)
  #fusion_users.unscale <- left_join(fusion, stuhr_fusion.users)
  
  return(fusion_users)
  
}



killDbConnections <- function () {
  
  all_cons <- dbListConnections(PostgreSQL())
  
  print(all_cons)
  
  for(con in all_cons)
    +  dbDisconnect(con)
  
  print(paste(length(all_cons), " connections killed."))
  
}

load_aubervilliers_and_stuhr <- function(){
  # create a connection
  # save the password that we can "hide" it as best as we can by collapsing it
  pw <- {
    "postgres"
  }
  
  # loads the PostgreSQL driver
  drv <- dbDriver("PostgreSQL")
  # creates a connection to the postgres database
  # note that "con" will be used later in each connection to the database
  
  # Connexion ? la base des donn?es Stuhr
  con <- dbConnect(drv, dbname = "bremen",
                   host = "localhost", port = 5432,
                   user = "postgres", password = "postgres")
  
  stuhr <- dbGetQuery(con, "SELECT * from indicators.stuhr")
  
  stuhr_v <- dbGetQuery(con, "SELECT * from indicators.stuhr_v")
  
  stuhr_fusion.users <- dbGetQuery(con, "SELECT DISTINCT ON (B.uid)
                                   B.*
                                   FROM
                                   indicators.stuhr_fusion A
                                   LEFT JOIN indicators.users_stuhr B ON A.uid = B.uid;")
  
  # Connexion ? la base des donn?es Aubervilliers
  con <- dbConnect(drv, dbname = "idf",
                   host = "localhost", port = 5432,
                   user = "postgres", password = "postgres")
  rm(pw) # removes the password
  
  aubervilliers <- dbGetQuery(con, "SELECT * from indicators.aubervilliers")
  
  aubervilliers_v <- dbGetQuery(con, "SELECT * from indicators.aubervilliers_vandalism")
  aubervilliers_v$vandalism_type <- NULL
  
  aubervilliers_fusion.users <- dbGetQuery(con, "SELECT DISTINCT ON (B.uid)
                                           B.*
                                           FROM
                                           indicators.aubervilliers A
                                           LEFT JOIN indicators.users_aubervilliers B ON A.uid = B.uid;")
  aubervilliers_fusion.fake_users <- dbGetQuery(con, "SELECT DISTINCT ON (B.uid)
                                                B.*
                                                FROM
                                                indicators.aubervilliers_vandalism A
                                                LEFT JOIN indicators.users_aubervilliers B ON A.uid = B.uid;")
  
  aubervilliers_fusion.users <- rbind(aubervilliers_fusion.users, aubervilliers_fusion.fake_users)
  
  # Renommer colonnes n_semesters
  colnames(aubervilliers_fusion.users)[colnames(aubervilliers_fusion.users)=="n_semesters_auberv"] <- "n_semesters"
  colnames(stuhr_fusion.users)[colnames(stuhr_fusion.users)=="n_semesters_bremen"] <- "n_semesters"
  
  # Concat?nation de tous les contributeurs de Stuhr et Aubervilliers
  all_users <- rbind(aubervilliers_fusion.users, stuhr_fusion.users) 
  all_users$avg <- (all_users$total_contributions + all_users$p_modification 
                    + all_users$p_delete + all_users$p_is_used
                    + all_users$p_is_edited + all_users$p_is_deleted
                    + all_users$nbWeeks + all_users$focalisation)/8
  all_users$weighted_avg <- (all_users$total_contributions + all_users$p_modification 
                             + all_users$p_delete + 2*all_users$p_is_used
                             + 2*all_users$p_is_edited + 2*all_users$p_is_deleted
                             + 3*all_users$nbWeeks + 3*all_users$focalisation)/15
  
  
  # Centrage et reduction des variables
  all_users_scale <- as.data.frame(all_users$uid) 
  names(all_users_scale)[1]<-"uid"
  all_users_scale$total_contributions <- scale(all_users$total_contributions)
  all_users_scale$p_creation <- scale(all_users$p_creation)
  all_users_scale$p_modification <- scale(all_users$p_modification)
  all_users_scale$p_delete <- scale(all_users$p_delete)
  all_users_scale$p_is_used <- scale(all_users$p_is_used)
  all_users_scale$p_is_edited <- scale(all_users$p_is_edited)
  all_users_scale$p_is_deleted <- scale(all_users$p_is_deleted)
  all_users_scale$nbWeeks <- scale(all_users$nbWeeks)
  all_users_scale$focalisation <- scale(all_users$focalisation)
  
  
  all_users_scale$avg <- (all_users_scale$total_contributions + all_users_scale$p_modification 
                          + all_users_scale$p_delete + all_users_scale$p_is_used
                          + all_users_scale$p_is_edited + all_users_scale$p_is_deleted
                          + all_users_scale$nbWeeks + all_users_scale$focalisation)/8
  all_users_scale$weighted_avg <- (all_users_scale$total_contributions + all_users_scale$p_modification 
                                   + all_users_scale$p_delete + 2*all_users_scale$p_is_used
                                   + 2*all_users_scale$p_is_edited + 2*all_users_scale$p_is_deleted
                                   + 3*all_users_scale$nbWeeks + 3*all_users_scale$focalisation)/15
  
  
  stuhr$vandalism = FALSE
  stuhr_v$vandalism=  TRUE
  fusion <- rbind(stuhr, stuhr_v)
  rm(stuhr)
  rm(stuhr_v)
  
  # Jointure des indicateurs contributeurs sur les batiments de la table fusion
  require("dplyr")
  fusion_users_stuhr <- left_join(fusion, all_users_scale)
  fusion_users_stuhr$dataset <- "stuhr"
  
  
  aubervilliers$vandalism = FALSE
  aubervilliers_v$vandalism=  TRUE
  # On retire la colonne vandalism_type, qui  n'existe pas dans la table aubervilliers
  aubervilliers_v$vandalism_type <- NULL
  
  fusion <- rbind(aubervilliers, aubervilliers_v)
  rm(aubervilliers)
  rm(aubervilliers_v)
  fusion_users_aubervilliers <- left_join(fusion, all_users_scale)
  fusion_users_aubervilliers$dataset <- "aubervilliers"
  
  # Supprimer colonne min_dist_surf_bati_bdtopo
  drop <- c("min_dist_surf_bati_bdtopo")
  fusion_users_aubervilliers = fusion_users_aubervilliers[,!(names(fusion_users_aubervilliers) %in% drop)]
  
  # Renommer colonnes n_semesters
  colnames(fusion_users_aubervilliers)[colnames(fusion_users_aubervilliers)=="n_semesters_auberv"] <- "n_semesters"
  colnames(fusion_users_stuhr)[colnames(fusion_users_stuhr)=="n_semesters_bremen"] <- "n_semesters"
  
  # Concat?nation des b?timents sur Stuhr et Aubervilliers
  total <- rbind(fusion_users_aubervilliers, fusion_users_stuhr)
  return(total) 
}



load_aubervilliers_and_stuhr_unscale <- function(){
  # create a connection
  # save the password that we can "hide" it as best as we can by collapsing it
  pw <- {
    "postgres"
  }
  
  # loads the PostgreSQL driver
  drv <- dbDriver("PostgreSQL")
  # creates a connection to the postgres database
  # note that "con" will be used later in each connection to the database
  
  # Connexion ? la base des donn?es Stuhr
  con <- dbConnect(drv, dbname = "bremen",
                   host = "localhost", port = 5432,
                   user = "postgres", password = "postgres")
  
  stuhr <- dbGetQuery(con, "SELECT * from indicators.stuhr")
  
  stuhr_v <- dbGetQuery(con, "SELECT * from indicators.stuhr_v")
  
  stuhr_fusion.users <- dbGetQuery(con, "SELECT DISTINCT ON (B.uid)
                                   B.*
                                   FROM
                                   indicators.stuhr_fusion A
                                   LEFT JOIN indicators.users_stuhr B ON A.uid = B.uid;")
  
  # Connexion a la base des donnees Aubervilliers
  con <- dbConnect(drv, dbname = "idf",
                   host = "localhost", port = 5432,
                   user = "postgres", password = "postgres")
  rm(pw) # removes the password
  
  aubervilliers <- dbGetQuery(con, "SELECT * from indicators.aubervilliers")
  
  aubervilliers_v <- dbGetQuery(con, "SELECT * from indicators.aubervilliers_vandalism")
  aubervilliers_v$vandalism_type <- NULL
  
  aubervilliers_fusion.users <- dbGetQuery(con, "SELECT DISTINCT ON (B.uid)
                                           B.*
                                           FROM
                                           indicators.aubervilliers A
                                           LEFT JOIN indicators.users_aubervilliers B ON A.uid = B.uid;")
  aubervilliers_fusion.fake_users <- dbGetQuery(con, "SELECT DISTINCT ON (B.uid)
                                                B.*
                                                FROM
                                                indicators.aubervilliers_vandalism A
                                                LEFT JOIN indicators.users_aubervilliers B ON A.uid = B.uid;")
  
  aubervilliers_fusion.users <- rbind(aubervilliers_fusion.users, aubervilliers_fusion.fake_users)
  
  # Renommer colonnes n_semesters
  colnames(aubervilliers_fusion.users)[colnames(aubervilliers_fusion.users)=="n_semesters_auberv"] <- "n_semesters"
  colnames(stuhr_fusion.users)[colnames(stuhr_fusion.users)=="n_semesters_bremen"] <- "n_semesters"
  
  # Concat?nation de tous les contributeurs de Stuhr et Aubervilliers
  all_users <- rbind(aubervilliers_fusion.users, stuhr_fusion.users) 
  all_users$avg <- (all_users$total_contributions + all_users$p_modification 
                    + all_users$p_delete + all_users$p_is_used
                    + all_users$p_is_edited + all_users$p_is_deleted
                    + all_users$nbWeeks + all_users$focalisation)/8
  all_users$weighted_avg <- (all_users$total_contributions + all_users$p_modification 
                             + all_users$p_delete + 2*all_users$p_is_used
                             + 2*all_users$p_is_edited + 2*all_users$p_is_deleted
                             + 3*all_users$nbWeeks + 3*all_users$focalisation)/15
  
  
  # Centrage et r?duction des variables
  all_users_unscale <- as.data.frame(all_users$uid) 
  names(all_users_unscale)[1]<-"uid"
  all_users_unscale$total_contributions <- all_users$total_contributions
  all_users_unscale$p_creation <- all_users$p_creation
  all_users_unscale$p_modification <- all_users$p_modification
  all_users_unscale$p_delete <- all_users$p_delete
  all_users_unscale$p_is_used <- all_users$p_is_used
  all_users_unscale$p_is_edited <- all_users$p_is_edited
  all_users_unscale$p_is_deleted <- all_users$p_is_deleted
  all_users_unscale$nbWeeks <- all_users$nbWeeks
  all_users_unscale$focalisation <- all_users$focalisation
  
  
  all_users_unscale$avg <- (all_users_unscale$total_contributions + all_users_unscale$p_modification 
                            + all_users_unscale$p_delete + all_users_unscale$p_is_used
                            + all_users_unscale$p_is_edited + all_users_unscale$p_is_deleted
                            + all_users_unscale$nbWeeks + all_users_unscale$focalisation)/8
  all_users_unscale$weighted_avg <- (all_users_unscale$total_contributions + all_users_unscale$p_modification 
                                     + all_users_unscale$p_delete + 2*all_users_unscale$p_is_used
                                     + 2*all_users_unscale$p_is_edited + 2*all_users_unscale$p_is_deleted
                                     + 3*all_users_unscale$nbWeeks + 3*all_users_unscale$focalisation)/15
  
  
  stuhr$vandalism = FALSE
  stuhr_v$vandalism=  TRUE
  fusion <- rbind(stuhr, stuhr_v)
  rm(stuhr)
  rm(stuhr_v)
  
  # Jointure des indicateurs contributeurs sur les batiments de la table fusion
  require("dplyr")
  fusion_users_stuhr <- left_join(fusion, all_users_unscale)
  fusion_users_stuhr$dataset <- "stuhr"
  
  
  aubervilliers$vandalism = FALSE
  aubervilliers_v$vandalism=  TRUE
  # On retire la colonne vandalism_type, qui  n'existe pas dans la table aubervilliers
  aubervilliers_v$vandalism_type <- NULL
  
  fusion <- rbind(aubervilliers, aubervilliers_v)
  rm(aubervilliers)
  rm(aubervilliers_v)
  fusion_users_aubervilliers <- left_join(fusion, all_users_unscale)
  fusion_users_aubervilliers$dataset <- "aubervilliers"
  
  # Supprimer colonne min_dist_surf_bati_bdtopo
  drop <- c("min_dist_surf_bati_bdtopo")
  fusion_users_aubervilliers = fusion_users_aubervilliers[,!(names(fusion_users_aubervilliers) %in% drop)]
  
  # Renommer colonnes n_semesters
  colnames(fusion_users_aubervilliers)[colnames(fusion_users_aubervilliers)=="n_semesters_auberv"] <- "n_semesters"
  colnames(fusion_users_stuhr)[colnames(fusion_users_stuhr)=="n_semesters_bremen"] <- "n_semesters"
  
  # Concatenation des batiments sur Stuhr et Aubervilliers
  total <- rbind(fusion_users_aubervilliers, fusion_users_stuhr)
  return(total) 
}