export class CaseInsensitiveMap<V> extends Map<string, V> {
  get (key: string): V|undefined {
    return super.get(key.toLowerCase?.())
  }

  has (key: string): boolean {
    return super.has(key.toLowerCase?.())
  }
  
  set (key: string, value: V) {
    return super.set(key.toLowerCase?.(), value)
  }
  
  delete (key: string) {
    return super.delete(key.toLowerCase?.())
  }
}