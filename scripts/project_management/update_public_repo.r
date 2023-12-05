#############################################################################
## Update_public_repo: Script to programmatically stage and commit only the
## files of the LULCC_CH project that should be added to the public repository
##
## Script creates a list of all files in the private local dir and then saves a
## copy to be edited manually, after this is done the 1st time new files should
## be added to the csv file manually.
##
## Date: 27-08-2023
## Author: Ben Black
#############################################################################

library("git2r")

#Based on the structure of two local dirs: Private and public, each with
#seperate local and remote git repositories. The private dir contains more
#files than the public and is used for dev. Updated files are then moved to the
#public dir before committed to the public repo and pushed to remote.
#Using 'git2r' package to automate staging and committing of only certain files.

root_dir <- dirname(getwd())

Private_dir <- paste0(root_dir, "/LULCC_CH")
Public_dir <- paste0(root_dir, "/LULCC_CH_public")

### =========================================================================
### Copying public files to local directory
### =========================================================================

#copy whole directory structure to public without files
#function to copy empty directory structure from one location to another
# copydirstruct <- function(from,to, exclusions){
#
#   #list dirs
#   oridir <- list.dirs(from,full.names = F)
#
#   #remove any exclusions
#   oridir <- oridir[!grepl(paste(exclusions, collapse="|"), oridir)]
#
#   #loop over dirs creating
#   for(i in 2:length(oridir)){
#     dir.create(paste0(to,'/',oridir[i]))
#   }
# }
# copydirstruct(from=Private_dir,
#               to=Public_dir,
#               exclusions = c("LULCC_CH_user_files",
#                              "PopulationModel_ValPar",
#                              "publication",
#                              "Scenario_trans-rates",
#                              "Transition_datasets_MC",
#                              ".git",
#                              ".Rproj.user"))

#vector of only the relevant sub-dirs in private repo
#containing files to be moved to public
sub_dirs <- c("Scripts",
              "Tools",
              "Model",
              "Data",
              "Results")

#save text file listing paths of files meant for the public repo
File_list_path <- paste0(Public_dir, "/File_transfer_list.csv")
 All_files <- list.files(paste0(Private_dir, "/", sub_dirs), recursive = TRUE, full.names = TRUE)
# write.csv(All_files,
#           file = File_list_path,
#           row.names = FALSE,
#           col.names = NA)

#Editing the text file manually is unfortunately the best way to pick out
#only the necessary files

#LOad in vector of required file paths
Public_files <- unlist(read.csv2(File_list_path, header = FALSE))

#check file sizes for any large files that will cause problems when commiting to Github

#For caution use 5MB (in bytes) as a size to issue a warning at
Git_max_size <- 5000000

#calculate file sizes
File_sizes <- sapply(Public_files, function(x){
file_size <- file.info(paste0(Private_dir, "/", x))$size
})
names(File_sizes) <- Public_files

#conditional to identify number and names of large files
if(any(File_sizes >= Git_max_size)){
  paste("Warning", length(File_sizes[which(File_sizes >= Git_max_size)]), "files greater than 5MB in size present:", names(File_sizes[which(File_sizes >= 789139)]))
  }else{"All files below 5MB"}

#create directories before moving files
sapply(dirname(Public_files), function(x){
  if(dir.exists(paste0(Public_dir, "/", x)) == FALSE){
    dir.create(paste0(Public_dir, "/", x), recursive = TRUE)
  }
})

#copy files from private dir to public
file.copy(from = paste0(Private_dir, "/", Public_files),
          to = paste0(Public_dir, "/", Public_files),
          overwrite = TRUE)

### =========================================================================
### Staging and commiting files
### =========================================================================

#add credentials for remote pushing
#creds <- cred_user_pass(username, password)

#connect to local repo
local_repo <- repository(paste0(Public_dir, "/.git"))

#add files (staging)
add(local_repo, Public_files)

#function to perform commit asking for user confirmation and message
user_confirm_commit <- function(){

  #prompt for confirmation of commit
  commit_log <- readline(prompt = "Commits are hard to undo are you sure you want to commit: Y/N:")

  #If positive then ask for commit message
  if(commit_log == "Y"){
    commit_msg <- readline(prompt = "Please enter a commit message:")
    commit(local_repo, commit_msg)
    }
}
user_confirm_commit()

### =========================================================================
### Push to remote
### =========================================================================

#FUNCTION RETURNS ERRORS WITHOUT CREDENTIALS AND WITH A CRED_USER_PASS OBJECT SUPPLIED
#MAYBE TRY CREATING SSH KEY?
#OTHERWISE: PUSH MANUALLY FROM GIT WINDOW
#push to remote
push(local_repo, name = "origin", "refs/heads/main", credentials = creds)
