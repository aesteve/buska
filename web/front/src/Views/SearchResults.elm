module Views.SearchResults exposing (..)

import Commands exposing (Msg)
import Html exposing (..)
import Html.Attributes exposing (..)
import Model exposing (State)


searchResults : State -> Html Msg
searchResults state =
    div [ id "matched" ] (List.map display_matching state.results.matches)


display_matching : String -> Html msg
display_matching matched =
    textarea [ disabled True ] [ text matched ]
