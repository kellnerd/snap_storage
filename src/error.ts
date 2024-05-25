/** Error which was caused by a {@linkcode fetch} request. */
export class ResponseError extends Error {
  constructor(message: string, public response: Response) {
    super(message);
    Object.defineProperty(this, "name", {
      value: "ResponseError",
      enumerable: false,
    });
  }
}
