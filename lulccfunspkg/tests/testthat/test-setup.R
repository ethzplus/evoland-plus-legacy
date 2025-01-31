test_that("get_config runs without errors", {
  config <- get_config()

  # if regionalization is set, then everything else should have gone ok
  testthat::expect_contains(config, list(regionalization = TRUE))
})
