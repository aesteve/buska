module Commands exposing (..)

import Json.Decode exposing (Error)
import Model exposing (Progress, SearchProgress, SearchStep, decodeSearchProgress, decodeSearchStep)


type Msg
    = StartSearch
    | MatchFound String
    | SearchStepChanged SearchStep
    | SearchProgressed SearchProgress
    | DecodingError Error


receivedSearchProgress : Json.Decode.Value -> Msg
receivedSearchProgress json =
    case Json.Decode.decodeValue decodeSearchProgress json of
        Ok decoded ->
            SearchProgressed decoded

        Err message ->
            DecodingError message


updateMatches : String -> Msg
updateMatches m =
    MatchFound m


receivedSearchStep : Json.Decode.Value -> Msg
receivedSearchStep json =
    case Json.Decode.decodeValue decodeSearchStep json of
        Ok decoded ->
            SearchStepChanged decoded

        Err message ->
            DecodingError message
