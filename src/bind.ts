'use strict';

export function autoBind(obj: Record<string, any>): void {
  const propertyNames = Object.getOwnPropertyNames(obj.constructor.prototype);

  propertyNames.forEach((propertyName) => {
    const value = (obj as any)[propertyName];

    if (propertyName !== 'constructor' && typeof value === 'function') {
      (obj as any)[propertyName] = value.bind(obj);
    }
  });
}
