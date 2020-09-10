source("retrieve_data_functions.R")

##########################
###### SCALED DATA  #######
##########################

# load the model
super_model_stuhr <- readRDS("./final_model_stuhr_trained_scale.rds")
print(super_model_stuhr)

# LANNILIS
# scaled data
lannilis_scale  <- load_data("bretagne", "lannilis")
vandalisme_l <- select(lannilis_scale, perimeter, shortest_length, median_length, 
                       elongation, convexity,
                       compacity, n_is_within_lulc, n_inter_lulc, max_special_char_ratio,
                       max_special_char_ratio, n_tags, vandalism, n_users, timespan_to_previous, 
                       avg, weighted_avg
)

for (i in 1:ncol(vandalisme_l)) {
  vandalisme_l[, i] <-  vandalisme_l[, i]
}
vandalisme_l$vandalism <- factor(ifelse(vandalisme_l$vandalism,"yes","no"))

vandalism_rf_pred <- predict(super_model_stuhr, vandalisme_l)
confusionMatrix(vandalism_rf_pred, as.factor(vandalisme_l$vandalism))


# HEILSBRONN
heilsbronn_scale  <- load_data("heilsbronn", "heilsbronn")
vandalisme_h <- select(heilsbronn_scale, perimeter, shortest_length, median_length, 
                       elongation, convexity,
                       compacity, n_is_within_lulc, n_inter_lulc, max_special_char_ratio,
                       max_special_char_ratio, n_tags, vandalism, n_users, timespan_to_previous, 
                       avg, weighted_avg
)

for (i in 1:ncol(vandalisme_h)) {
  vandalisme_h[, i] <-  vandalisme_h[, i]
}
vandalisme_h$vandalism <- factor(ifelse(vandalisme_h$vandalism,"yes","no"))

vandalism_rf_pred <- predict(super_model_stuhr, vandalisme_h)
confusionMatrix(vandalism_rf_pred, as.factor(vandalisme_h$vandalism))


# Aubervilliers
auberv_scale <- data_aubervilliers_loading()

vandalisme_a <- select(auberv_scale, perimeter, shortest_length, median_length, 
                       elongation, convexity,
                       compacity, n_is_within_lulc, n_inter_lulc, max_special_char_ratio,
                       max_special_char_ratio, n_tags, vandalism, n_users, timespan_to_previous, 
                       avg, weighted_avg
)

for (i in 1:ncol(vandalisme_h)) {
  vandalisme_a[, i] <-  vandalisme_a[, i]
}
vandalisme_a$vandalism <- factor(ifelse(vandalisme_a$vandalism,"yes","no"))

vandalism_rf_pred <- predict(super_model_stuhr, vandalisme_a)
confusionMatrix(vandalism_rf_pred, as.factor(vandalisme_a$vandalism))

# Stuhr
stuhr_scale <- load_data("bremen", "stuhr")

vandalisme_s <- select(stuhr_scale, perimeter, shortest_length, median_length, 
                       elongation, convexity,
                       compacity, n_is_within_lulc, n_inter_lulc, max_special_char_ratio,
                       max_special_char_ratio, n_tags, vandalism, n_users, timespan_to_previous, 
                       avg, weighted_avg
)

for (i in 1:ncol(vandalisme_h)) {
  vandalisme_s[, i] <-  vandalisme_s[, i]
}
vandalisme_s$vandalism <- factor(ifelse(vandalisme_s$vandalism,"yes","no"))

vandalism_rf_pred <- predict(super_model_stuhr, vandalisme_s)
confusionMatrix(vandalism_rf_pred, as.factor(vandalisme_s$vandalism))



##########################
###### UNSCALED DATA  #####
##########################
# load the model
super_model_stuhr_unscale <- readRDS("./final_model_stuhr_trained_unscale.rds")
print(super_model_stuhr_unscale)

# LANNILIS
lannilis_unscale  <- load_data_non_centre_reduit("bretagne", "lannilis")
vandalisme_l <- select(lannilis_unscale, perimeter, shortest_length, median_length, 
                       elongation, convexity,
                       compacity, n_is_within_lulc, n_inter_lulc, max_special_char_ratio,
                       max_special_char_ratio, n_tags, vandalism, n_users, timespan_to_previous, 
                       avg, weighted_avg
)

for (i in 1:ncol(vandalisme_l)) {
  vandalisme_l[, i] <-  vandalisme_l[, i]
}
vandalisme_l$vandalism <- factor(ifelse(vandalisme_l$vandalism,"yes","no"))

vandalism_rf_pred <- predict(super_model_stuhr_unscale, vandalisme_l)
confusionMatrix(vandalism_rf_pred, as.factor(vandalisme_l$vandalism))

# HEILSBRONN
heilsbronn_unscale  <- load_data_non_centre_reduit("heilsbronn", "heilsbronn")
vandalisme_h <- select(heilsbronn_unscale, perimeter, shortest_length, median_length, 
                       elongation, convexity,
                       compacity, n_is_within_lulc, n_inter_lulc, max_special_char_ratio,
                       max_special_char_ratio, n_tags, vandalism, n_users, timespan_to_previous, 
                       avg, weighted_avg
)

for (i in 1:ncol(vandalisme_h)) {
  vandalisme_h[, i] <-  vandalisme_h[, i]
}
vandalisme_h$vandalism <- factor(ifelse(vandalisme_h$vandalism,"yes","no"))

vandalism_rf_pred <- predict(super_model_stuhr_unscale, vandalisme_h)
confusionMatrix(vandalism_rf_pred, as.factor(vandalisme_h$vandalism))

# Aubervilliers
killDbConnections()
auberv_unscale <- data_aubervilliers_loading_non_centre_reduit()

vandalisme_a <- select(auberv_unscale, perimeter, shortest_length, median_length, 
                       elongation, convexity,
                       compacity, n_is_within_lulc, n_inter_lulc, max_special_char_ratio,
                       max_special_char_ratio, n_tags, vandalism, n_users, timespan_to_previous, 
                       avg, weighted_avg
)

for (i in 1:ncol(vandalisme_a)) {
  vandalisme_a[, i] <-  vandalisme_a[, i]
}
vandalisme_a$vandalism <- factor(ifelse(vandalisme_a$vandalism,"yes","no"))

vandalism_rf_pred <- predict(super_model_stuhr_unscale, vandalisme_a)
confusionMatrix(vandalism_rf_pred, as.factor(vandalisme_a$vandalism))

# Stuhr
stuhr_unscale <- load_data_non_centre_reduit("bremen", "stuhr")

vandalisme_s <- select(stuhr_unscale, perimeter, shortest_length, median_length, 
                       elongation, convexity,
                       compacity, n_is_within_lulc, n_inter_lulc, max_special_char_ratio,
                       max_special_char_ratio, n_tags, vandalism, n_users, timespan_to_previous, 
                       avg, weighted_avg
)

for (i in 1:ncol(vandalisme_s)) {
  vandalisme_s[, i] <-  vandalisme_s[, i]
}
vandalisme_s$vandalism <- factor(ifelse(vandalisme_s$vandalism,"yes","no"))

vandalism_rf_pred <- predict(super_model_stuhr_unscale, vandalisme_s)
confusionMatrix(vandalism_rf_pred, as.factor(vandalisme_s$vandalism))


##########################
###  SANS CONTRIBUTEUR ###
##########################
# load the model
super_model_stuhr_sans_contributeur <- readRDS("./final_model_stuhr_trained_sans_variable_contributeur.rds")
print(super_model_stuhr_sans_contributeur)

# LANNILIS
lannilis_sans_contributeur  <- load_data_non_centre_reduit("bretagne", "lannilis")
vandalisme_l <- select(lannilis_sans_contributeur, perimeter, shortest_length, median_length, 
                       elongation, convexity,
                       compacity, n_is_within_lulc, n_inter_lulc, max_special_char_ratio,
                       max_special_char_ratio, n_tags, vandalism, n_users, timespan_to_previous
)

for (i in 1:ncol(vandalisme_l)) {
  vandalisme_l[, i] <-  vandalisme_l[, i]
}
vandalisme_l$vandalism <- factor(ifelse(vandalisme_l$vandalism,"yes","no"))

vandalism_rf_pred <- predict(super_model_stuhr_sans_contributeur, vandalisme_l)
confusionMatrix(vandalism_rf_pred, as.factor(vandalisme_l$vandalism))

# HEILSBRONN
lannilis_sans_contributeur  <- load_data_non_centre_reduit("heilsbronn", "heilsbronn")
vandalisme_h <- select(lannilis_sans_contributeur, perimeter, shortest_length, median_length, 
                       elongation, convexity,
                       compacity, n_is_within_lulc, n_inter_lulc, max_special_char_ratio,
                       max_special_char_ratio, n_tags, vandalism, n_users, timespan_to_previous
)

for (i in 1:ncol(vandalisme_h)) {
  vandalisme_h[, i] <-  vandalisme_h[, i]
}
vandalisme_h$vandalism <- factor(ifelse(vandalisme_h$vandalism,"yes","no"))

vandalism_rf_pred <- predict(super_model_stuhr_sans_contributeur, vandalisme_h)
confusionMatrix(vandalism_rf_pred, as.factor(vandalisme_h$vandalism))

# Aubervilliers
killDbConnections()
auberv_sans_contrib <- data_aubervilliers_loading_non_centre_reduit()

vandalisme_a_sans_contrib <- select(auberv_sans_contrib, perimeter, shortest_length, median_length, 
                                    elongation, convexity,
                                    compacity, n_is_within_lulc, n_inter_lulc, max_special_char_ratio,
                                    max_special_char_ratio, n_tags, vandalism, n_users, timespan_to_previous
)

for (i in 1:ncol(vandalisme_a_sans_contrib)) {
  vandalisme_a_sans_contrib[, i] <-  vandalisme_a_sans_contrib[, i]
}
vandalisme_a_sans_contrib$vandalism <- factor(ifelse(vandalisme_a_sans_contrib$vandalism,"yes","no"))

vandalism_rf_pred <- predict(super_model_stuhr_sans_contributeur, vandalisme_a_sans_contrib)
confusionMatrix(vandalism_rf_pred, as.factor(vandalisme_a_sans_contrib$vandalism))

# Stuhr
stuhr_sans_contrib <- load_data_non_centre_reduit("bremen", "stuhr")

vandalisme_s_sans_contrib <- select(stuhr_sans_contrib, perimeter, shortest_length, median_length, 
                                    elongation, convexity,
                                    compacity, n_is_within_lulc, n_inter_lulc, max_special_char_ratio,
                                    max_special_char_ratio, n_tags, vandalism, n_users, timespan_to_previous
)

for (i in 1:ncol(vandalisme_s_sans_contrib)) {
  vandalisme_s_sans_contrib[, i] <-  vandalisme_s_sans_contrib[, i]
}
vandalisme_s_sans_contrib$vandalism <- factor(ifelse(vandalisme_s_sans_contrib$vandalism,"yes","no"))

vandalism_rf_pred <- predict(super_model_stuhr_sans_contributeur, vandalisme_s_sans_contrib)
confusionMatrix(vandalism_rf_pred, as.factor(vandalisme_s_sans_contrib$vandalism))

# Aubervilliers + Stuhr
auberv_stuhr_validation_data_sans_contributeur <- read.csv(file="./aubervilliers_stuhr_validation_data_sans_variables_contributeurs.csv", header=TRUE, sep=",")
vandalism_rf_pred <- predict(super_model_sans_contributeur, auberv_stuhr_validation_data_sans_contributeur)
confusionMatrix(vandalism_rf_pred, as.factor(auberv_stuhr_validation_data_unscale$vandalism))
