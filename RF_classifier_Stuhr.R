source("retrieve_data_functions.R")

killDbConnections()
stuhr_data_scale <- load_data("bremen", "stuhr")

vandalisme_total <- select(stuhr_data_scale, perimeter, shortest_length, median_length, 
                           elongation, convexity,
                           compacity, n_is_within_lulc, n_inter_lulc, max_special_char_ratio,
                           max_special_char_ratio, n_tags, vandalism, n_users, timespan_to_previous,
                           avg, weighted_avg
)

for (i in 1:ncol(vandalisme_total)) {
  vandalisme_total[, i] <-  vandalisme_total[, i]
}
vandalisme_total$vandalism <- factor(ifelse(vandalisme_total$vandalism,"yes","no"))

str(vandalisme_total)


# load libraries
library(caret)
library(mlbench)
library(randomForest)

# create 80%/20% for training and validation datasets
validation_index <- createDataPartition(vandalisme_total$vandalism, p=0.80, list=FALSE)
validation <- vandalisme_total[-validation_index,]
training <- vandalisme_total[validation_index,]

# train a model and summarize model
set.seed(7)
control <- trainControl(method="repeatedcv", number=10, repeats=3)
fit.rf <- train(vandalism~., data=training, method="rf", metric="Accuracy", trControl=control)
print(fit.rf)
print(fit.rf$finalModel)

# create standalone model using all training data
set.seed(7)
finalModel <- randomForest(vandalism~., training, mtry=7, ntree=500)
# make a predictions on "new data" using the final model
final_predictions <- predict(finalModel, validation)
confusionMatrix(final_predictions, validation$vandalism)

# save the model to disk
saveRDS(finalModel, "./final_model_stuhr_trained_sans_variable_contributeur.rds")

# Save validation set as a CSV file
write.csv(validation, file = "./stuhr_validation_data_sans_variable_contributeur.csv")