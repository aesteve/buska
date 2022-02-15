module Home exposing (main)

import Browser
import Commands exposing (Msg(..), receivedSearchProgress, receivedSearchStep, updateMatches)
import Html exposing (..)
import Html.Attributes exposing (..)
import Model exposing (..)
import Ports.JSSearch exposing (matchFound, searchProgressed, searchStepChanged, startSearch)
import Views.SearchForm exposing (searchForm)
import Views.SearchProgress exposing (searchProgress)
import Views.SearchResults exposing (searchResults)


view : State -> Html Msg
view state =
    div [ class "main" ]
        [ h1 [] [ text "Welcome to BusKa" ]
        , searchForm state
        , searchProgress state
        , searchResults state
        ]


init : () -> ( State, Cmd Msg )
init _ =
    ( { definition = Nothing, results = resetSearchResults }, Cmd.none )


update : Msg -> State -> ( State, Cmd Msg )
update msg state =
    case msg of
        StartSearch ->
            ( state, startSearch () )

        SearchProgressed searchProgress ->
            ( { state | results = updateProgress state.results searchProgress }, Cmd.none )

        SearchStepChanged newSearchStep ->
            ( { state | results = updateStep state.results newSearchStep }, Cmd.none )

        DecodingError error ->
            let
                _ =
                    Debug.log "Error in decoding JSON " error
            in
            ( state, Cmd.none )

        MatchFound string ->
            ( { state | results = appendMatch state.results string }, Cmd.none )


subscriptions : State -> Sub Msg
subscriptions state =
    Sub.batch
        [ searchProgressed receivedSearchProgress
        , matchFound updateMatches
        , searchStepChanged receivedSearchStep
        ]


main : Program () State Msg
main =
    Browser.element
        { init = init
        , view = view
        , update = update
        , subscriptions = subscriptions
        }
