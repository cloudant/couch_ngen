% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.
-module(rfc3394).

%% This is not secure enough for production use but it was easy to build and
%% will suffice as scaffolding. Must be replaced before production!
%% RFC 5297 is a possibility, but Zurich team will recommend something soon.

-export([wrap/2, unwrap/2]).

wrap(Key, PlainText)
  when bit_size(PlainText) >= 128,
       bit_size(PlainText) rem 64 == 0 ->
    Checksum = <<16#A6A6A6A6A6A6A6A6:64>>,
    Blocks = split(PlainText),
    wrap(Key, Checksum, 1, 0, Blocks, []).


wrap(_Key, Checksum, I, 5, Blocks, Acc) when I > length(Blocks) ->
    <<Checksum/binary, (iolist_to_binary(lists:reverse(Acc)))/binary>>;

wrap(Key, Checksum, I, J, Blocks, Acc) when I > length(Blocks) ->
    wrap(Key, Checksum, 1, J + 1, lists:reverse(Acc), []);

wrap(Key, Checksum, I, J, Blocks, Acc) ->
    NthBlock = lists:nth(I, Blocks),
    <<MSB:64, LSB:64>> = crypto:block_encrypt(aes_ecb,
        Key, <<Checksum/binary, NthBlock/binary>>),
    wrap(Key, crypto:exor(<<MSB:64>>, <<((length(Blocks) * J) + I):64>>),
         I + 1, J, Blocks, [<<LSB:64>> | Acc]).


unwrap(Key, CipherText)
  when bit_size(CipherText) >= 192,
       bit_size(CipherText) rem 64 == 0 ->
    [Checksum | Blocks] = split(CipherText),
    unwrap(Key, Checksum, length(Blocks), 5, Blocks, []).

unwrap(_Key, Checksum, 0, 0, Blocks, Acc) ->
    case Checksum of
        <<16#A6A6A6A6A6A6A6A6:64>> ->
            iolist_to_binary(Acc);
        _ ->
            throw({invalid_checksum, Checksum, Blocks, Acc})
    end;

unwrap(Key, Checksum, 0, J, Blocks, Acc) ->
    unwrap(Key, Checksum, length(Blocks), J - 1, Acc, []);

unwrap(Key, Checksum, I, J, Blocks, Acc) ->
    NthBlock = lists:nth(I, Blocks),
    Xor = crypto:exor(Checksum, <<((length(Blocks) * J) + I):64>>),
    <<MSB:64, LSB:64>> = crypto:block_decrypt(aes_ecb,
        Key, <<Xor/binary, NthBlock/binary>>),
    unwrap(Key, <<MSB:64>>, I - 1, J, Blocks, [<<LSB:64>> | Acc]).


split(Bin) when bit_size(Bin) rem 64 == 0 ->
    split(Bin, []).

split(<<>>, Acc) ->
    lists:reverse(Acc);

split(<<H:64, T/binary>>, Acc) ->
    split(T, [<<H:64>> | Acc]).

-ifdef(TEST).
-include_lib("couch/include/couch_eunit.hrl").

split_test_() ->
    [
     ?_assertEqual([<<0:64>>, <<1:64>>], split(<<0:64, 1:64>>)),
     ?_assertEqual([<<0:64>>, <<1:64>>, <<2:64>>], split(<<0:64, 1:64, 2:64>>)),
     ?_assertEqual([<<0:64>>, <<1:64>>, <<2:64>>, <<3:64>>], split(<<0:64, 1:64, 2:64, 3:64>>))
    ].

wrap_test_() ->
    lists:flatten(
      [
       assertWrap(<<16#000102030405060708090A0B0C0D0E0F:128>>,
                  <<16#00112233445566778899AABBCCDDEEFF:128>>,
                  <<16#1FA68B0A8112B447AEF34BD8FB5A7B829D3E862371D2CFE5:192>>),

       assertWrap(<<16#000102030405060708090A0B0C0D0E0F1011121314151617:192>>,
                  <<16#00112233445566778899AABBCCDDEEFF:128>>,
                  <<16#96778B25AE6CA435F92B5B97C050AED2468AB8A17AD84E5D:192>>),

       assertWrap(<<16#000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F:256>>,
                  <<16#00112233445566778899AABBCCDDEEFF:128>>,
                  <<16#64E8C3F9CE0F5BA263E9777905818A2A93C8191E7D6E8AE7:192>>),

       assertWrap(<<16#000102030405060708090A0B0C0D0E0F1011121314151617:192>>,
                  <<16#00112233445566778899AABBCCDDEEFF0001020304050607:192>>,
                  <<16#031D33264E15D33268F24EC260743EDCE1C6C7DDEE725A936BA814915C6762D2:256>>),

       assertWrap(<<16#000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F:256>>,
                  <<16#00112233445566778899AABBCCDDEEFF0001020304050607:192>>,
                  <<16#A8F9BC1612C68B3FF6E6F4FBE30E71E4769C8B80A32CB8958CD5D17D6B254DA1:256>>),

       assertWrap(<<16#000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F:256>>,
                  <<16#00112233445566778899AABBCCDDEEFF000102030405060708090A0B0C0D0E0F:256>>,
                  <<16#28C9F404C4B810F4CBCCB35CFB87F8263F5786E2D80ED326CBC7F0E71A99F43BFB988B9B7A02DD21:320>>)
      ]).

assertWrap(Key, PlainText, CipherText) ->
    [?_assertEqual(CipherText, wrap(Key, PlainText)),
     ?_assertEqual(PlainText, unwrap(Key, CipherText))].

-endif.
