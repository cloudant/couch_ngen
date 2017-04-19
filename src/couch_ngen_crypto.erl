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

-module(couch_ngen_crypto).
%% not a gen_server so as not to leak key data
-vsn(1).

-export([start_link/1, start_link/2, encrypt/3, decrypt/3]).

-record(state, {key_id, key}).

-export([init/1, init/2]).

%% public functions

start_link(KeyId) ->
    proc_lib:start_link(?MODULE, init, [KeyId]).

start_link(KeyId, WrappedKey) ->
    proc_lib:start_link(?MODULE, init, [KeyId, WrappedKey]).


encrypt(Pid, IV, Bin) ->
    call(Pid, {encrypt, IV, Bin}).


decrypt(Pid, IV, Bin) ->
    call(Pid, {decrypt, IV, Bin}).


%% gen_server functions

init(KeyId) ->
    process_flag(sensitive, true),
    MasterKey = fetch_master_key(KeyId),
    FileKey = generate_file_key(MasterKey),
    WrappedKey = wrap_file_key(KeyId, MasterKey, FileKey),
    State = #state{key_id = KeyId, key = FileKey},
    proc_lib:init_ack({ok, self(), WrappedKey}),
    loop(State).

init(KeyId, WrappedKey) ->
    process_flag(sensitive, true),
    MasterKey = fetch_master_key(KeyId),
    case unwrap_file_key(KeyId, MasterKey, WrappedKey) of
        {ok, FileKey} ->
            State = #state{key_id = KeyId, key = FileKey},
            proc_lib:init_ack({ok, self()}),
            loop(State);
        {error, Reason} ->
            proc_lib:init_ack({error, Reason})
    end.


loop(State) ->
    receive
        {Pid, Ref, {encrypt, IV, Bin}} ->
            Pid ! {Ref, gcm_encrypt(State#state.key, IV, Bin)},
            loop(State);

        {Pid, Ref, {decrypt, IV, Bin}} ->
            Pid ! {Ref, gcm_decrypt(State#state.key, IV, Bin)},
            loop(State);

        _Other ->
            loop(State)
    end.


%% private functions

call(Pid, Msg) ->
    Ref = erlang:make_ref(),
    link(Pid),
    Pid ! {self(), Ref, Msg},
    receive
        {Ref, Reply} ->
            unlink(Pid),
            Reply
    end.


fetch_master_key(KeyId) ->
    %% TODO fetch key from backend.
    crypto:hash(sha256, KeyId).


generate_file_key(MasterKey) ->
    crypto:strong_rand_bytes(byte_size(MasterKey)).


wrap_file_key(KeyId, MasterKey, FileKey) ->
    {CipherText, <<CipherTag:16/binary>>} =
        siv:encrypt(expand(MasterKey), [KeyId], FileKey),
    <<CipherTag:16/binary, CipherText/binary>>.


unwrap_file_key(KeyId, MasterKey, WrappedKey) ->
    <<CipherTag:16/binary,
      CipherText/binary>> = WrappedKey,
    case siv:decrypt(expand(MasterKey), [KeyId], {CipherText, CipherTag}) of
        error ->
            {error, decryption_failed};
        FileKey ->
            {ok, FileKey}
    end.


gcm_encrypt(Key, IV, PlainText)
  when IV >= 0, IV =< 16#1000000000000000000000000 ->
    {CipherText, CipherTag} = crypto:block_encrypt(
       aes_gcm, Key, <<IV:96>>, {<<>>, PlainText, 16}),
    <<CipherTag:16/binary, CipherText/binary>>.


gcm_decrypt(Key, IV, <<CipherTag:16/binary, CipherText/binary>>)
  when IV >= 0, IV =< 16#1000000000000000000000000 ->
    crypto:block_decrypt(
        aes_gcm, Key, <<IV:96>>, {<<>>, CipherText, CipherTag}).


%% because SIV only uses half the bits of the input key
%% to encrypt and the other half for the authentication/IV
%% we expand our keys to 512 to ensure an overall security
%% threshold of 256.
expand(Key) when bit_size(Key) == 512 ->
    Key; % 512 is enough for anyone.
expand(Key) when bit_size(Key) == 256 ->
    %% expansion technique from Bjoern Tackmann - IBM Zurich
    K0 = crypto:block_encrypt(aes_ecb, Key, <<0:128>>),
    K1 = crypto:block_encrypt(aes_ecb, Key, <<1:128>>),
    K2 = crypto:block_encrypt(aes_ecb, Key, <<2:128>>),
    K3 = crypto:block_encrypt(aes_ecb, Key, <<3:128>>),
    <<K0/binary, K1/binary, K2/binary, K3/binary>>.
