require(testthat)

# nolint start
sample_dinamica_script_encoded <- '
@charset = UTF-8
@date = 2023-Oct-13 15:04:52
@version = 8.3
Script {{
    realValue1652 := RealValue 2;

    @collapsed = no
    calculateRexpression1653 := CalculateRExpression "b3V0cHV0IDwtIHYxKjIKb3V0cHV0RG91YmxlKCJvdXRwdXRfbnVtYmVyIiwgb3V0cHV0KQ==" .no {{
        NumberValue realValue1652 1;
    }};

    @viewer.number = yes
    _ := ExtractStructNumber calculateRexpression1653 $"(output_number)";
}};
'

sample_dinamica_script_decoded <- '
@charset = UTF-8
@date = 2023-Oct-13 15:04:52
@version = 8.3
Script {{
    realValue1652 := RealValue 2;

    @collapsed = no
    calculateRexpression1653 := CalculateRExpression "evoland::some_function_call()" .no {{
        NumberValue realValue1652 1;
    }};

    @viewer.number = yes
    _ := ExtractStructNumber calculateRexpression1653 $"(output_number)";
}};
'
# nolint end

test_that("process_dinamica_script encodes correctly", {
  # pretty-print process_dinamica_script calls using cat()
  expect_error(
    process_dinamica_script(
      I(sample_dinamica_script_encoded),
      mode = "encode"
    ),
    "seems unlikely for an unencoded code chunk"
  )
  expect_match(
    process_dinamica_script(
      I(sample_dinamica_script_encoded),
      mode = "decode"
    ),
    'output <- v1\\*2\\noutputDouble\\("output_number", output\\)'
  )
})

test_that("process_dinamica_script decodes correctly", {
  # pretty-print process_dinamica_script calls using cat()
  expect_error(
    process_dinamica_script(
      I(sample_dinamica_script_decoded),
      mode = "decode"
    ),
    "seems unlikely for an encoded code chunk"
  )
  expect_match(
    process_dinamica_script(
      I(sample_dinamica_script_decoded),
      mode = "encode"
    ),
    "ZXZvbGFuZDo6c29tZV9mdW5jdGlvbl9jYWxsKCk="
  )
})

test_that("process_dinamica_script is idempotent", {
  expect_equal(
    {
      sample_dinamica_script_encoded |>
        I() |>
        process_dinamica_script(mode = "decode") |>
        I() |>
        process_dinamica_script(mode = "encode")
    },
    sample_dinamica_script_encoded
  )
  expect_equal(
    {
      sample_dinamica_script_decoded |>
        I() |>
        process_dinamica_script(mode = "encode") |>
        I() |>
        process_dinamica_script(mode = "decode")
    },
    sample_dinamica_script_decoded
  )
})
