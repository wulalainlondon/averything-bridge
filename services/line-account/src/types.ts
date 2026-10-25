export interface RawMessage { id?:string; from?:string; to?:string; text?:string; contentType?:string|number; createdTime?:string|number|bigint;deliveredTime?:string|number|bigint; chunks?:Uint8Array[]; contentMetadata?:Record<string,string> }
export interface Base {
 profile:{mid:string;displayName?:string};
 relation:{getUserFriendIds(input:unknown):Promise<{userFriendMids?:string[]}>};
 talk:{getProfile():Promise<{mid:string;displayName?:string}>;getContacts(input:unknown):Promise<{mid:string;displayName:string;displayNameOverridden?:string;statusMessage?:string;picturePath?:string;pictureStatus?:string}[]>;getPreviousMessagesV2WithRequest(input:unknown):Promise<RawMessage[]>;getRecentMessagesV2(input:unknown):Promise<RawMessage[]>;negotiateE2EEPublicKey(input:unknown):Promise<{specVersion?:number;publicKey?:unknown}>;sendMessage(input:unknown):Promise<RawMessage>;sync(input:unknown):Promise<SyncResult>;sendChatChecked(input:unknown):Promise<unknown>};
 obs:{constructor:Function;downloadMediaByE2EE(input:RawMessage):Promise<File|null>};
 e2ee:{decryptE2EEMessage(input:RawMessage):Promise<RawMessage>;encryptE2EEMessage(to:string,text:string,type:string):Promise<Uint8Array[]>};
 square:{getSquare(input:unknown):Promise<Record<string,unknown>>;fetchMyEvents(input:unknown):Promise<Record<string,unknown>>;getSquareChat(input:unknown):Promise<Record<string,unknown>>;fetchSquareChatEvents(input:unknown):Promise<Record<string,unknown>>};
 getReqseq(name?:string):Promise<number>;
}
export interface SyncResult {fullSyncResponse?:{nextRevision?:number|bigint};operationResponse?:{globalEvents?:{lastRevision?:number|bigint};individualEvents?:{lastRevision?:number|bigint};operations?:{type:string;revision:number|bigint;param1?:string;param2?:string;message?:RawMessage}[]}}
export interface Runtime {base:Base;storage:{get(key:string):Promise<unknown>;set(key:string,value:unknown):Promise<void>};close():Promise<void>}
export interface WireMessage {message_ref:string;target_ref:string;direction:string;text?:string;content_type:string;timestamp:number}
export interface ServiceEvent {kind:string;event_key:string;target:string;cursor?:string;message?:WireMessage;code?:string}
export class LineFailure extends Error { constructor(readonly code:string,readonly beforeEffect=true){super(code)} }
export function object(value:unknown):Record<string,unknown>{if(!value||typeof value!=='object'||Array.isArray(value))throw new LineFailure('line_arguments_invalid');return value as Record<string,unknown>}
export function text(value:unknown,max=5000):string{if(typeof value!=='string'||!value.trim()||value.length>max)throw new LineFailure('line_arguments_invalid');return value}
export function integer(value:unknown,min:number,max:number):number{if(typeof value!=='number'||!Number.isSafeInteger(value)||value<min||value>max)throw new LineFailure('line_arguments_invalid');return value}
export function errorCode(error:unknown):string{return error instanceof LineFailure?error.code:'line_runtime_failed'}
