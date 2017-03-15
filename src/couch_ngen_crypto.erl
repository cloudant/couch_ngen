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
    %% SIV uses half the master key bits to wrap the data
    %% so there's no point making the file key longer than that.
    crypto:strong_rand_bytes(byte_size(MasterKey) div 2).


wrap_file_key(KeyId, MasterKey, FileKey) ->
    {CipherText, <<CipherTag:16/binary>>} =
        siv:encrypt(MasterKey, [KeyId], FileKey),
    <<CipherTag:16/binary, CipherText/binary>>.


unwrap_file_key(KeyId, MasterKey, WrappedKey) ->
    <<CipherTag:16/binary,
      CipherText/binary>> = WrappedKey,
    case siv:decrypt(MasterKey, [KeyId], {CipherText, CipherTag}) of
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
