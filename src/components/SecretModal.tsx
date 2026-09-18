import { useState } from "react";
import { copyText } from "../lib/clipboard";
import { Modal } from "./Modal";

export function SecretModal({
  secret,
  title,
  onClose,
}: {
  secret: string;
  title: string;
  onClose: () => void;
}) {
  const [copied, setCopied] = useState(false);
  const [copyError, setCopyError] = useState("");
  const copy = async () => {
    setCopyError("");
    try {
      if (!(await copyText(secret))) throw new Error("copy command was rejected");
      setCopied(true);
    } catch {
      setCopied(false);
      setCopyError(
        "Copy failed. Select the secret above and copy it manually.",
      );
    }
  };
  return (
    <Modal title={title} onClose={onClose}>
      <div className="secret-warning">
        <strong>Keep this secret private</strong>
        <span>
          Copy it only to a secure location. Anyone with this key can use its
          allowed models.
        </span>
      </div>
      <code className="secret-value">{secret}</code>
      {copyError && (
        <div className="field-error" role="alert">
          {copyError}
        </div>
      )}
      <div className="dialog-actions">
        <button className="button" onClick={() => void copy()}>
          {copied ? "Copied" : "Copy secret"}
        </button>
        <button className="button secondary" onClick={onClose}>
          Close
        </button>
      </div>
    </Modal>
  );
}
