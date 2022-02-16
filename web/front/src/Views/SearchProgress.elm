module Views.SearchProgress exposing (..)

import Commands exposing (Msg)
import Dict
import FormatNumber exposing (format)
import FormatNumber.Locales exposing (Decimals(..), System(..), usLocale)
import Html exposing (..)
import Html.Attributes exposing (..)
import Model exposing (Progress, SearchResults, SearchState(..), SearchStep, State)


percentFormat =
    { usLocale | decimals = Exact 2 }


searchProgress : State -> Html Msg
searchProgress state =
    div
        []
        [ searchSteps state.results
        , searchProgressBar state.results
        ]


progressBar : Progress -> Html Msg
progressBar p =
    progress
        [ class "full-progress-blue"
        , Html.Attributes.min "0"
        , p.done |> String.fromInt |> value
        , p.total |> String.fromInt |> Html.Attributes.max
        ]
        [ text (format percentFormat (p.rate * 100) ++ "%") ]


partitionProgressBar : String -> Progress -> List (Html Msg) -> List (Html Msg)
partitionProgressBar part p list =
    list ++ [ li [] [ span [] [ text (part ++ ":") ], progressBar p ] ]


searchProgressBar : SearchResults -> Html Msg
searchProgressBar state =
    div []
        (case state.progress of
            InProgress currentProgress ->
                [ progressBar currentProgress.overallProgress
                , ul [ class "no-style-list" ] (Dict.foldl partitionProgressBar [] currentProgress.perPartitionProgress)
                ]

            Finished ->
                [ progress
                    [ class "full-progress-blue"
                    , Html.Attributes.min "0"
                    , value "100"
                    , Html.Attributes.max "100"
                    ]
                    [ text "100%" ]
                ]

            _ ->
                []
        )


searchSteps : SearchResults -> Html Msg
searchSteps results =
    ul [ class "no-style-list" ] (List.map searchStep results.steps)


searchStep : SearchStep -> Html Msg
searchStep step =
    li [] [ text (String.fromInt step.step ++ ". " ++ step.description) ]
