package kr.co.penta.datalake.common;

import java.security.spec.KeySpec;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

public class Aes {
    private static final int keySize = 128;
    private static final int iterationCount = 10000;
    private static String salt = "79752f1d3fd2432043c48e45b35b24645eb826a25c6f1804e9152665c345a552";
    private static String iv = "2fad5a477d13ecda7f718fbd8a9f0443";
    private static final String passPhrase = "==Penta!System==";
    private final Cipher cipher;

    public Aes() {
        try {
            cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String encrypt(String plaintext) throws Exception {
        return encrypt(salt, iv, passPhrase, plaintext);
    }

    public String decrypt(String ciphertext) throws Exception {
        return decrypt(salt, iv, passPhrase, ciphertext);
    }

    private String encrypt(String salt, String iv, String passPhrase, String plaintext) throws Exception {
        SecretKey key = generateKey(salt, passPhrase);
        byte[] encrypted = doFinal(Cipher.ENCRYPT_MODE, key, iv, plaintext.getBytes("UTF-8"));
        return encodeBase64(encrypted);
    }

    private String decrypt(String salt, String iv, String passPhrase, String ciphertext) throws Exception {
        SecretKey key = generateKey(salt, passPhrase);
        byte[] decrypted = doFinal(Cipher.DECRYPT_MODE, key, iv, decodeBase64(ciphertext));
        return new String(decrypted, "UTF-8");
    }

    private byte[] doFinal(int encryptMode, SecretKey key, String iv, byte[] bytes) throws Exception {
        cipher.init(encryptMode, key, new IvParameterSpec(decodeHex(iv)));
        return cipher.doFinal(bytes);
    }

    private SecretKey generateKey(String salt, String passPhrase) throws Exception {
        SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
        KeySpec spec = new PBEKeySpec(passPhrase.toCharArray(), decodeHex(salt), iterationCount, keySize);
        SecretKey key = new SecretKeySpec(factory.generateSecret(spec).getEncoded(), "AES");
        return key;
    }

    private static String encodeBase64(byte[] bytes) {
        return Base64.encodeBase64String(bytes);
    }

    private static byte[] decodeBase64(String str) {
        return Base64.decodeBase64(str);
    }

    private static byte[] decodeHex(String str) throws Exception {
        return Hex.decodeHex(str.toCharArray());
    }

    public static String password(String password) {
        if (password.startsWith("aes:")) {
            Aes aes = new Aes();
            try {
                return aes.decrypt(password.substring(4));
            } catch (Exception e) {
                System.err.println(e);
                System.exit(1);
            }
        }
        return password;
    }

    public static void main(String[] args) {

        Aes aes = new Aes();

        try {
            for (String token : args) {
                if (token.startsWith("aes:")) {
                    token = token.substring(4);
                    System.out.println(aes.decrypt(token));
                } else {
                    System.out.print("aes:" + aes.encrypt(token));
                }
            }
        } catch (Exception e) {
            System.err.println(e);
            System.exit(1);
        }
    }
}