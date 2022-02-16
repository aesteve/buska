module Views.SearchForm exposing (..)

import Commands exposing (Msg(..))
import Html exposing (..)
import Html.Attributes exposing (..)
import Html.Events exposing (onClick)
import Model exposing (State)


searchForm : State -> Html Msg
searchForm state =
    div []
        [ div [ class "card" ]
            [ div [ class "card-divider" ] [ text "Search options" ]
            , div [ class "card-section" ]
                [ div [ class "grid-container full" ]
                    [ div [ class "grid-x grid-padding-x" ]
                        [ div [ class "medium-9 cell" ]
                            [ label [ for "host" ]
                                [ text "Host"
                                , input [ type_ "text", id "host", placeholder "localhost" ] []
                                ]
                            ]
                        , div [ class "medium-3 cell" ]
                            [ label [ for "port" ]
                                [ text "Port"
                                , input [ type_ "number", id "port", placeholder "9092" ] []
                                ]
                            ]
                        ]
                    ]
                ]
            , div [ class "card-section" ]
                [ button [ class "button", onClick StartSearch ] [ text "Search" ]
                ]
            ]
        ]
