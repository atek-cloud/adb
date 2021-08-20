
/**
 * File generated by Atek tsgen
 * env=node-userland
 * DO NOT MODIFY
 */
import { URL } from 'url';
import { AtekRpcServer, AtekRpcServerHandlers } from '@atek-cloud/node-rpc';

export const ID = "atek.cloud/adb-api";
export const REVISION = undefined;
const SCHEMAS = {"$schema":"http://json-schema.org/draft-07/schema#","definitions":{"AdbApi":{"type":"object"},"DbSubscription":{"type":"object"},"DbDescription":{"type":"object","properties":{"dbId":{"type":"string"},"dbType":{"type":"string"},"displayName":{"type":"string"},"tables":{"type":"array","items":{"$ref":"#/definitions/TableDescription"}}},"required":["dbId","dbType","tables"]},"TableDescription":{"type":"object","properties":{"revision":{"type":"number"},"templates":{"$ref":"#/definitions/TableTemplates"},"definition":{"type":"object"},"tableId":{"type":"string"}},"required":["tableId"]},"TableSettings":{"type":"object","properties":{"revision":{"type":"number"},"templates":{"$ref":"#/definitions/TableTemplates"},"definition":{"type":"object"}}},"TableTemplates":{"type":"object","properties":{"table":{"type":"object","properties":{"title":{"type":"string"},"description":{"type":"string"}}},"record":{"type":"object","properties":{"key":{"type":"string"},"title":{"type":"string"},"description":{"type":"string"}}}}},"Record":{"type":"object","properties":{"key":{"type":"string"},"path":{"type":"string"},"url":{"type":"string"},"seq":{"type":"number"},"value":{"anyOf":[{"type":"object"},{"type":"null"}]}},"required":["key","path","url"]},"BlobMap":{"type":"object","additionalProperties":{"$ref":"#/definitions/BlobDesc"}},"BlobDesc":{"type":"object","properties":{"mimeType":{"type":"string"},"buf":{"type":"string","contentEncoding":"base64"}},"required":["buf"]},"Blob":{"type":"object","properties":{"start":{"type":"number"},"end":{"type":"number"},"mimeType":{"type":"string"},"buf":{"type":"string","contentEncoding":"base64"}},"required":["start","end","buf"]},"Diff":{"type":"object","properties":{"left":{"$ref":"#/definitions/Record"},"right":{"$ref":"#/definitions/Record"}},"required":["left","right"]},"ListOpts":{"type":"object","properties":{"lt":{"type":"string"},"lte":{"type":"string"},"gt":{"type":"string"},"gte":{"type":"string"},"limit":{"type":"number"},"reverse":{"type":"boolean"}}},"api_AdbApi_Describe":{"type":"object","properties":{"params":{"type":"array","items":{"type":"string"},"minItems":1,"maxItems":1},"returns":{"$ref":"#/definitions/DbDescription"}},"required":["params","returns"]},"api_AdbApi_Table":{"type":"object","properties":{"params":{"type":"array","minItems":3,"items":[{"type":"string"},{"type":"string"},{"$ref":"#/definitions/TableSettings"}],"maxItems":3},"returns":{"$ref":"#/definitions/TableDescription"}},"required":["params","returns"]},"api_AdbApi_List":{"type":"object","properties":{"params":{"type":"array","minItems":2,"items":[{"type":"string"},{"type":"string"},{"$ref":"#/definitions/ListOpts"}],"maxItems":3},"returns":{"type":"object","properties":{"records":{"type":"array","items":{"$ref":"#/definitions/Record"}}},"required":["records"]}},"required":["params","returns"]},"api_AdbApi_Get":{"type":"object","properties":{"params":{"type":"array","items":{"type":"string"},"minItems":3,"maxItems":3},"returns":{"$ref":"#/definitions/Record"}},"required":["params","returns"]},"api_AdbApi_Create":{"type":"object","properties":{"params":{"type":"array","minItems":3,"items":[{"type":"string"},{"type":"string"},{"type":"object"},{"$ref":"#/definitions/BlobMap"}],"maxItems":4},"returns":{"$ref":"#/definitions/Record"}},"required":["params","returns"]},"api_AdbApi_Put":{"type":"object","properties":{"params":{"type":"array","minItems":4,"items":[{"type":"string"},{"type":"string"},{"type":"string"},{"type":"object"}],"maxItems":4},"returns":{"$ref":"#/definitions/Record"}},"required":["params","returns"]},"api_AdbApi_Delete":{"type":"object","properties":{"params":{"type":"array","items":{"type":"string"},"minItems":3,"maxItems":3},"returns":{"type":"null"}},"required":["params","returns"]},"api_AdbApi_Diff":{"type":"object","properties":{"params":{"type":"array","minItems":2,"items":[{"type":"string"},{"type":"object","properties":{"left":{"type":"number"},"right":{"type":"number"},"tableIds":{"type":"array","items":{"type":"string"}}},"required":["left"]}],"maxItems":2},"returns":{"type":"array","items":{"$ref":"#/definitions/Diff"}}},"required":["params","returns"]},"api_AdbApi_GetBlob":{"type":"object","properties":{"params":{"type":"array","items":{"type":"string"},"minItems":4,"maxItems":4},"returns":{"$ref":"#/definitions/Blob"}},"required":["params","returns"]},"api_AdbApi_PutBlob":{"type":"object","properties":{"params":{"type":"array","minItems":5,"items":[{"type":"string"},{"type":"string"},{"type":"string"},{"type":"string"},{"$ref":"#/definitions/BlobDesc"}],"maxItems":5},"returns":{"type":"null"}},"required":["params","returns"]},"api_AdbApi_DelBlob":{"type":"object","properties":{"params":{"type":"array","items":{"type":"string"},"minItems":4,"maxItems":4},"returns":{"type":"null"}},"required":["params","returns"]},"api_AdbApi_Subscribe":{"type":"object","properties":{"params":{"type":"array","minItems":1,"items":[{"type":"string"},{"type":"object","properties":{"tableIds":{"type":"array","items":{"type":"string"}}}}],"maxItems":2},"returns":{"$ref":"#/definitions/DbSubscription"}},"required":["params","returns"]},"evt_DbSubscription_Change":{"type":"object","properties":{"left":{"$ref":"#/definitions/Record"},"right":{"$ref":"#/definitions/Record"}},"required":["left","right"]}}};
const EXPORT_MAP = {"methods":{"describe":"#/definitions/api_AdbApi_Describe","table":"#/definitions/api_AdbApi_Table","list":"#/definitions/api_AdbApi_List","get":"#/definitions/api_AdbApi_Get","create":"#/definitions/api_AdbApi_Create","put":"#/definitions/api_AdbApi_Put","delete":"#/definitions/api_AdbApi_Delete","diff":"#/definitions/api_AdbApi_Diff","getBlob":"#/definitions/api_AdbApi_GetBlob","putBlob":"#/definitions/api_AdbApi_PutBlob","delBlob":"#/definitions/api_AdbApi_DelBlob","subscribe":"#/definitions/api_AdbApi_Subscribe"},"events":{"DbSubscription":{"change":"#/definitions/evt_DbSubscription_Change"}}};

export default class AdbApiServer extends AtekRpcServer {
  constructor(handlers: AtekRpcServerHandlers) {
    super(SCHEMAS, EXPORT_MAP, handlers)
  }
}
