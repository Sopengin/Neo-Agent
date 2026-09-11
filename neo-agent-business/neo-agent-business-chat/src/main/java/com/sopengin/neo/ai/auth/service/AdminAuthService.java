package com.sopengin.neo.ai.auth.service;

import jakarta.servlet.http.HttpServletRequest;
import com.sopengin.neo.ai.auth.dto.AdminLoginRequest;
import com.sopengin.neo.ai.auth.vo.AdminLoginVo;
import com.sopengin.neo.ai.auth.vo.AdminProfileVo;

/**
 * 后台登录认证服务。
 */
public interface AdminAuthService {

    AdminLoginVo login(AdminLoginRequest request);

    AdminProfileVo currentProfile(HttpServletRequest request);
}
